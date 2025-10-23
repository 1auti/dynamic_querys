package org.transito_seguro.component.processor.execution;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Ejecutor especializado en queries con STREAMING.
 *
 * Responsabilidades:
 * - Procesar datos en streaming (registro por registro)
 * - Usar buffer/chunks para eficiencia
 * - Prevenir OutOfMemoryError en datasets masivos
 * - Monitorear memoria continuamente
 *
 * Uso recomendado:
 * - Datasets > 100K registros
 * - Queries que no se pueden paginar
 * - Cuando no se conoce el tamaño total
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamingQueryExecutor {

    private final MemoryMonitor memoryMonitor;

    /**
     * Tamaño del chunk para procesar datos en streaming.
     * Configurable vía properties.
     */
    @Value("${app.batch.streaming-chunk-size:1000}")
    private int chunkSize;

    /**
     * Frecuencia de logging (cada N chunks).
     */
    @Value("${app.batch.streaming-log-frequency:10}")
    private int logFrequency;

    /**
     * Ejecuta query con streaming, procesando registro por registro.
     *
     * FLUJO:
     * 1. Configura filtros sin límite ni offset
     * 2. Ejecuta query con callback por registro
     * 3. Acumula registros en buffer (chunks)
     * 4. Procesa chunks cuando se llenan
     * 5. Libera memoria inmediatamente
     *
     * @param repo Repositorio de la provincia
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @param provincia Nombre de la provincia
     * @param contexto Contexto de procesamiento
     */
    public void ejecutar(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        try {
            log.info("🌊 Iniciando STREAMING para {}: chunks de {} registros",
                    provincia, chunkSize);

            // Configurar para streaming (sin límite ni offset)
            ParametrosFiltrosDTO filtrosStreaming = filtros.toBuilder()
                    .limite(null)
                    .offset(null)
                    .lastId(null)
                    .lastSerieEquipo(null)
                    .lastLugar(null)
                    .lastKeysetConsolidacion(null)
                    .build();

            // Buffer para acumular registros
            final List<Map<String, Object>> buffer = new ArrayList<>(chunkSize);
            final AtomicInteger totalProcesados = new AtomicInteger(0);
            final AtomicInteger chunksEnviados = new AtomicInteger(0);

            // Callback que se ejecuta por CADA registro
            Consumer<Map<String, Object>> procesarRegistro = registro -> {
                buffer.add(registro);

                // Cuando el buffer se llena, procesarlo
                if (buffer.size() >= chunkSize) {
                    procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
                }
            };

            // Ejecutar query con streaming (delegado al repositorio)
            repo.ejecutarQueryConStreaming(nombreQuery, filtrosStreaming, procesarRegistro);

            // Procesar registros restantes en el buffer
            if (!buffer.isEmpty()) {
                procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
            }

            log.info("✅ STREAMING completado para {}: {:,} registros en {} chunks | Memoria: {:.1f}%",
                    provincia,
                    totalProcesados.get(),
                    chunksEnviados.get(),
                    memoryMonitor.obtenerPorcentajeMemoriaUsada());

        } catch (OutOfMemoryError oom) {
            log.error("💥 OOM en STREAMING para {}: {}", provincia, oom.getMessage());
            throw new RuntimeException("OutOfMemoryError en streaming", oom);

        } catch (Exception e) {
            log.error("❌ Error en STREAMING para {}: {}", provincia, e.getMessage(), e);
            throw new RuntimeException("Error en streaming", e);
        }
    }

    /**
     * Procesa un chunk de registros y lo envía al contexto.
     *
     * CRÍTICO:
     * - Crear copia inmutable (no modificar buffer original)
     * - Vaciar buffer inmediatamente (liberar memoria)
     * - Pausar si memoria alta
     *
     * @param buffer Buffer con registros acumulados
     * @param provincia Nombre de la provincia
     * @param contexto Contexto de procesamiento
     * @param totalProcesados Contador total de registros
     * @param chunksEnviados Contador de chunks enviados
     */
    private void procesarChunk(
            List<Map<String, Object>> buffer,
            String provincia,
            ContextoProcesamiento contexto,
            AtomicInteger totalProcesados,
            AtomicInteger chunksEnviados) {

        if (buffer.isEmpty()) {
            return;
        }

        try {
            // Crear copia inmutable del chunk
            List<Map<String, Object>> chunkInmutable = crearCopiasInmutables(buffer, provincia);

            // Enviar al contexto para procesamiento
            contexto.agregarResultados(chunkInmutable);

            // Actualizar contadores
            int procesados = buffer.size();
            totalProcesados.addAndGet(procesados);
            int numChunk = chunksEnviados.incrementAndGet();

            // Log cada N chunks (configurable)
            if (numChunk % logFrequency == 0) {
                log.info("📦 {} - Chunk {}: {:,} registros procesados | Memoria: {:.1f}%",
                        provincia,
                        numChunk,
                        totalProcesados.get(),
                        memoryMonitor.obtenerPorcentajeMemoriaUsada());
            }

            // Vaciar buffer para liberar memoria INMEDIATAMENTE
            buffer.clear();

            // Pausa si memoria alta (dar tiempo al GC)
            if (memoryMonitor.esMemoriaAlta()) {
                log.debug("⏸️ {} - Pausando streaming (memoria alta: {:.1f}%)",
                        provincia, memoryMonitor.obtenerPorcentajeMemoriaUsada());
                memoryMonitor.pausarSiNecesario();
            }

            // Si memoria crítica, sugerir GC explícitamente
            if (memoryMonitor.esMemoriaCritica()) {
                log.warn("⚠️ {} - Memoria crítica en chunk {}, sugiriendo GC",
                        provincia, numChunk);
                memoryMonitor.sugerirGcSiNecesario();
            }

        } catch (Exception e) {
            log.error("❌ Error procesando chunk {} para {}: {}",
                    chunksEnviados.get(), provincia, e.getMessage());
            throw new RuntimeException("Error procesando chunk", e);
        }
    }

    /**
     * Crea copias inmutables de registros agregando provincia.
     *
     * IMPORTANTE:
     * - NO modificar registros originales
     * - Agregar campo "provincia"
     * - Excluir campos técnicos si es necesario
     *
     * @param registros Lista de registros
     * @param provincia Nombre de la provincia
     * @return Lista de copias inmutables
     */
    private List<Map<String, Object>> crearCopiasInmutables(
            List<Map<String, Object>> registros,
            String provincia) {

        return registros.stream()
                .map(registro -> {
                    Map<String, Object> copia = new HashMap<>();

                    // Copiar todos los campos EXCEPTO provincia
                    registro.entrySet().stream()
                            .filter(e -> !"provincia".equals(e.getKey()))
                            .filter(e -> !"row_id".equals(e.getKey())) // Excluir campos técnicos
                            .forEach(e -> copia.put(e.getKey(), e.getValue()));

                    // Agregar provincia
                    copia.put("provincia", provincia);

                    return copia;
                })
                .collect(Collectors.toList());
    }

    /**
     * Ejecuta query con streaming y transformación personalizada.
     *
     * Permite aplicar transformaciones adicionales a cada registro
     * antes de procesarlo.
     *
     * @param repo Repositorio de la provincia
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @param provincia Nombre de la provincia
     * @param contexto Contexto de procesamiento
     * @param transformacion Función de transformación opcional
     */
    public void ejecutarConTransformacion(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            java.util.function.Function<Map<String, Object>, Map<String, Object>> transformacion) {

        try {
            log.info("🌊 Iniciando STREAMING con transformación para {}", provincia);

            ParametrosFiltrosDTO filtrosStreaming = filtros.toBuilder()
                    .limite(null)
                    .offset(null)
                    .build();

            final List<Map<String, Object>> buffer = new ArrayList<>(chunkSize);
            final AtomicInteger totalProcesados = new AtomicInteger(0);
            final AtomicInteger chunksEnviados = new AtomicInteger(0);

            Consumer<Map<String, Object>> procesarRegistro = registro -> {
                // Aplicar transformación si existe
                Map<String, Object> registroTransformado = transformacion != null
                        ? transformacion.apply(registro)
                        : registro;

                if (registroTransformado != null) {
                    buffer.add(registroTransformado);

                    if (buffer.size() >= chunkSize) {
                        procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
                    }
                }
            };

            repo.ejecutarQueryConStreaming(nombreQuery, filtrosStreaming, procesarRegistro);

            if (!buffer.isEmpty()) {
                procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
            }

            log.info("✅ STREAMING con transformación completado para {}: {:,} registros",
                    provincia, totalProcesados.get());

        } catch (Exception e) {
            log.error("❌ Error en STREAMING con transformación para {}: {}",
                    provincia, e.getMessage(), e);
            throw new RuntimeException("Error en streaming con transformación", e);
        }
    }

    /**
     * Obtiene el tamaño de chunk configurado.
     *
     * @return Tamaño de chunk
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Establece un tamaño de chunk personalizado (útil para tests).
     *
     * @param chunkSize Nuevo tamaño de chunk
     */
    public void setChunkSize(int chunkSize) {
        if (chunkSize < 100 || chunkSize > 10000) {
            log.warn("⚠️ Tamaño de chunk fuera de rango: {} (debe ser 100-10000)", chunkSize);
            throw new IllegalArgumentException("Tamaño de chunk inválido: " + chunkSize);
        }

        this.chunkSize = chunkSize;
        log.info("🔧 Tamaño de chunk actualizado: {}", chunkSize);
    }
}