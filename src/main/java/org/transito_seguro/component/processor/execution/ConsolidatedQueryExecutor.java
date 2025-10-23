package org.transito_seguro.component.processor.execution;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Ejecutor especializado en queries CONSOLIDADAS (con GROUP BY).
 *
 * Responsabilidades:
 * - Ejecutar queries tipo AGREGACION (pocos registros)
 * - Ejecutar queries tipo CRUDO con streaming (muchos registros)
 * - Validar estimaciones y cambiar estrategia si es necesario
 * - Actualizar estimaciones en BD
 *
 * @author Transito Seguro Team
 * @version 3.0 - Extraído de BatchProcessor
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConsolidatedQueryExecutor {

    private final MemoryMonitor memoryMonitor;
    private final QueryStorageRepository queryStorageRepository;

    @Value("${consolidacion.agregacion.umbral-error:10}")
    private int umbralErrorEstimacion;

    @Value("${consolidacion.agregacion.limite-validacion:10000}")
    private int limiteValidacion;

    @Value("${consolidacion.agregacion.limite-absoluto:100000}")
    private int limiteAbsoluto;

    /**
     * Ejecuta query consolidada según su tipo.
     */
    public void ejecutar(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        TipoConsolidacion tipo = queryStorage.getTipoConsolidacion();

        if (tipo == TipoConsolidacion.AGREGACION) {
            // Query bien diseñada: carga directa con validación
            ejecutarAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);

        } else if (tipo == TipoConsolidacion.CRUDO) {
            // Query mal diseñada: streaming obligatorio
            ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);

        } else if (tipo == TipoConsolidacion.DEDUPLICACION) {
            // Decidir según estimación
            if (queryStorage.getRegistrosEstimados() != null &&
                    queryStorage.getRegistrosEstimados() < 10000) {
                ejecutarAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            } else {
                ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            }

        } else {
            // Tipo desconocido: usar estrategia conservadora
            log.warn("⚠️ Tipo de consolidación {} desconocido para {}, usando CRUDO",
                    tipo, nombreQuery);
            ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
        }
    }

    /**
     * Ejecuta query AGREGACION con validación de tamaño.
     *
     * PROTEGIDO contra OOM:
     * 1. Valida con límite antes de cargar todo
     * 2. Si detecta error de estimación, cambia a CRUDO
     * 3. Actualiza estimación en BD para futuras ejecuciones
     */
    private void ejecutarAgregacion(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        try {
            log.debug("🔍 Ejecutando AGREGACION para {}: {} registros estimados",
                    provincia, queryStorage.getRegistrosEstimados());

            Integer estimacion = queryStorage.getRegistrosEstimados();

            // VALIDACIÓN PRE-EJECUCIÓN
            if (estimacion == null || estimacion == 0) {
                log.warn("⚠️ Sin estimación confiable, usando CRUDO por seguridad");
                ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
                return;
            }

            if (estimacion > limiteValidacion) {
                log.info("📊 Estimación alta ({} registros), usando CRUDO", estimacion);
                ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
                return;
            }

            // EJECUCIÓN CON LÍMITE DE VALIDACIÓN
            ParametrosFiltrosDTO filtrosValidacion = filtros.toBuilder()
                    .limite(limiteValidacion)
                    .offset(null)
                    .build();

            log.debug("🧪 Validando con límite de {} registros", limiteValidacion);

            List<Map<String, Object>> muestraValidacion =
                    repo.ejecutarQueryConFiltros(nombreQuery, filtrosValidacion);

            if (muestraValidacion == null) {
                log.warn("⚠️ Query retornó null para {}", provincia);
                return;
            }

            int tamanoMuestra = muestraValidacion.size();

            // ANÁLISIS Y DECISIÓN
            if (tamanoMuestra >= limiteValidacion) {
                // La muestra está incompleta (hay más datos)
                log.warn("⚠️ Query retornó {} registros (límite alcanzado)", tamanoMuestra);

                if (tamanoMuestra > estimacion * umbralErrorEstimacion) {
                    log.error("❌ ERROR DE ESTIMACIÓN: {}x más registros de lo estimado",
                            tamanoMuestra / Math.max(estimacion, 1));
                    log.info("🔄 Cambiando a estrategia CRUDO (streaming)");

                    actualizarEstimacion(queryStorage, tamanoMuestra * 2);
                    ejecutarCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
                    return;
                }

                // Usar paginación para procesar todo
                log.info("📄 Usando paginación para procesar todos los registros");
                ejecutarConPaginacion(repo, filtros, nombreQuery, provincia, contexto, limiteValidacion);
                return;
            }

            // La muestra es completa
            log.debug("✅ Muestra completa: {} registros", tamanoMuestra);

            if (estimacion > 0 && tamanoMuestra > estimacion * umbralErrorEstimacion) {
                log.warn("⚠️ Discrepancia con estimación: esperados={}, reales={}",
                        estimacion, tamanoMuestra);
                actualizarEstimacion(queryStorage, tamanoMuestra);
            }

            if (tamanoMuestra == 0) {
                log.debug("📭 Query no retornó resultados para {}", provincia);
                return;
            }

            // PROCESAMIENTO NORMAL
            List<Map<String, Object>> resultadoInmutable =
                    crearCopiasInmutables(muestraValidacion, provincia);
            contexto.agregarResultados(resultadoInmutable);

            log.info("✅ AGREGACION completada para {}: {} registros | Memoria: {:.1f}%",
                    provincia, tamanoMuestra, memoryMonitor.obtenerPorcentajeMemoriaUsada());

        } catch (OutOfMemoryError oom) {
            log.error("💥 OOM en AGREGACION para {}. Revisar límites en properties.", provincia);
            throw new RuntimeException("OutOfMemoryError en consolidación AGREGACION", oom);

        } catch (Exception e) {
            log.error("❌ Error en AGREGACION {} para {}: {}",
                    nombreQuery, provincia, e.getMessage(), e);
        }
    }

    /**
     * Ejecuta query con paginación cuando el resultado es grande pero manejable.
     */
    private void ejecutarConPaginacion(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            int tamanoPagina) {

        log.info("📄 Iniciando paginación para {}: páginas de {} registros",
                provincia, tamanoPagina);
        int offset = 0;
        int totalProcesados = 0;
        int iteracion = 0;
        int maxIteraciones = limiteAbsoluto / tamanoPagina; // Límite de seguridad

        while (iteracion < maxIteraciones) {
            try {
                // Crear filtros para esta página
                ParametrosFiltrosDTO filtrosPagina = filtros.toBuilder()
                        .limite(tamanoPagina)
                        .offset(offset)
                        .build();

                // Ejecutar query
                List<Map<String, Object>> pagina =
                        repo.ejecutarQueryConFiltros(nombreQuery, filtrosPagina);

                // Verificar si terminamos
                if (pagina == null || pagina.isEmpty()) {
                    log.debug("✅ Paginación completada en iteración {}", iteracion);
                    break;
                }

                // Procesar página
                List<Map<String, Object>> paginaInmutable = crearCopiasInmutables(pagina, provincia);
                contexto.agregarResultados(paginaInmutable);

                totalProcesados += pagina.size();
                offset += tamanoPagina;
                iteracion++;

                log.debug("📄 Página {}: {} registros (total: {})",
                        iteracion, pagina.size(), totalProcesados);

                // Si la página no está llena, es la última
                if (pagina.size() < tamanoPagina) {
                    break;
                }

                // Pausa si memoria alta
                if (memoryMonitor.esMemoriaAlta()) {
                    memoryMonitor.pausarSiNecesario();
                }

            } catch (Exception e) {
                log.error("❌ Error en iteración {} de paginación: {}", iteracion, e.getMessage());
                break;
            }
        }

        if (iteracion >= maxIteraciones) {
            log.error("⚠️ Límite de iteraciones alcanzado ({}) para {}. Loop infinito evitado.",
                    maxIteraciones, provincia);
        }

        log.info("✅ Paginación completada para {}: {} registros en {} páginas",
                provincia, totalProcesados, iteracion);
    }

    /**
     * Ejecuta query CRUDO con streaming.
     *
     * Para queries mal diseñadas que retornan muchos registros sin agregar.
     * Usa streaming para evitar OutOfMemoryError.
     */
    private void ejecutarCrudo(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        try {
            log.info("🌊 Ejecutando CRUDO con streaming para {}: {} registros estimados",
                    provincia, queryStorage.getRegistrosEstimados());

            // Configurar para streaming
            ParametrosFiltrosDTO filtrosStreaming = filtros.toBuilder()
                    .limite(null)
                    .offset(null)
                    .build();

            // Buffer para procesar en chunks
            final int CHUNK_SIZE = 1000;
            final List<Map<String, Object>> buffer = new ArrayList<>(CHUNK_SIZE);
            final AtomicInteger totalProcesados = new AtomicInteger(0);
            final AtomicInteger chunksEnviados = new AtomicInteger(0);

            // Callback que se ejecuta por cada registro
            Consumer<Map<String, Object>> procesarRegistro = registro -> {
                buffer.add(registro);

                // Cuando el buffer se llena, procesarlo
                if (buffer.size() >= CHUNK_SIZE) {
                    procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
                }
            };

            // Ejecutar query con streaming
            repo.ejecutarQueryConStreaming(nombreQuery, filtrosStreaming, procesarRegistro);

            // Procesar registros restantes en el buffer
            if (!buffer.isEmpty()) {
                procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
            }

            log.info("✅ CRUDO completado para {}: {} registros en {} chunks | Memoria: {:.1f}%",
                    provincia,
                    totalProcesados.get(),
                    chunksEnviados.get(),
                    memoryMonitor.obtenerPorcentajeMemoriaUsada());

        } catch (Exception e) {
            log.error("❌ Error en CRUDO para {}: {}", provincia, e.getMessage(), e);
        }
    }

    /**
     * Procesa un chunk de registros y lo envía al contexto.
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

        // Crear copia inmutable del chunk
        List<Map<String, Object>> chunkInmutable = crearCopiasInmutables(buffer, provincia);

        // Enviar al contexto
        contexto.agregarResultados(chunkInmutable);

        // Actualizar contadores
        int procesados = buffer.size();
        totalProcesados.addAndGet(procesados);
        chunksEnviados.incrementAndGet();

        // Log cada 10 chunks (10,000 registros)
        if (chunksEnviados.get() % 10 == 0) {
            log.debug("📊 Provincia {}: {} registros procesados en {} chunks | Memoria: {:.1f}%",
                    provincia,
                    totalProcesados.get(),
                    chunksEnviados.get(),
                    memoryMonitor.obtenerPorcentajeMemoriaUsada());
        }

        // Vaciar buffer para liberar memoria
        buffer.clear();

        // Pausa si memoria alta
        if (memoryMonitor.esMemoriaAlta()) {
            memoryMonitor.pausarSiNecesario();
        }
    }

    /**
     * Actualiza la estimación de registros en QueryStorage.
     */
    private void actualizarEstimacion(QueryStorage queryStorage, int registrosReales) {
        try {
            log.info("🔄 Actualizando estimación para {}: {} → {}",
                    queryStorage.getCodigo(),
                    queryStorage.getRegistrosEstimados(),
                    registrosReales);

            queryStorage.setRegistrosEstimados(registrosReales);
            queryStorageRepository.save(queryStorage);

            log.debug("✅ Estimación actualizada en BD");

        } catch (Exception e) {
            log.warn("⚠️ Error actualizando estimación: {}", e.getMessage());
        }
    }

    /**
     * Crea copias inmutables de registros agregando provincia.
     */
    private List<Map<String, Object>> crearCopiasInmutables(
            List<Map<String, Object>> registros,
            String provincia) {

        return registros.stream()
                .map(registro -> {
                    Map<String, Object> copia = new HashMap<>();
                    registro.entrySet().stream()
                            .filter(e -> !"provincia".equals(e.getKey()))
                            .forEach(e -> copia.put(e.getKey(), e.getValue()));
                    copia.put("provincia", provincia);
                    return copia;
                })
                .collect(Collectors.toList());
    }
}