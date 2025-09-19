package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.batch.size:1000}")
    private int defaultBatchSize;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryPerBatchMB;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${app.batch.chunk-size:250}")
    private int chunkSize;

    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double memoryCriticalThreshold;

    /**
     * Procesa datos en lotes de manera eficiente
     */
    // Método corregido en BatchProcessor.java
    /**
     * Procesa datos en lotes de manera eficiente con gestión de memoria mejorada
     */
    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("Iniciando procesamiento en lotes para {} repositorios", repositories.size());

        int batchSize = calcularTamanoLoteOptimo(filtros);
        int totalProcesados = 0;
        int totalRepositorios = repositories.size();
        int repositorioActual = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            repositorioActual++;
            String provincia = repo.getProvincia();
            log.info("Procesando provincia {}/{}: {}", repositorioActual, totalRepositorios, provincia);

            int offset = 0;
            boolean hayMasDatos = true;
            int procesadosEnProvincia = 0;

            while (hayMasDatos && !esMemoriaCritica()) {
                try {
                    // Verificar memoria ANTES de procesar
                    if (esMemoriaAlta()) {
                        log.warn("Memoria alta detectada antes de procesar lote. Provincia: {}, Offset: {}",
                                provincia, offset);
                        pausaInteligente();
                    }

                    // Crear filtros específicos para este lote
                    ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset);

                    // Ejecutar consulta para este lote
                    List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                    if (lote == null || lote.isEmpty()) {
                        log.debug("No hay más datos en provincia: {}, offset: {}", provincia, offset);
                        hayMasDatos = false;
                        break;
                    }

                    int tamanoLoteActual = lote.size();

                    // Agregar información de provincia a cada registro EN CHUNKS para evitar overhead
                    procesarProvinciaEnChunks(lote, provincia);

                    log.debug("Procesando lote: provincia={}, offset={}, tamaño={}, memoria={}%",
                            provincia, offset, tamanoLoteActual, obtenerPorcentajeMemoriaUsada());

                    // Procesar el lote en chunks pequeños con liberación frecuente
                    procesarLoteEnChunksConLiberacion(lote, procesadorLote);

                    // Actualizar contadores
                    procesadosEnProvincia += tamanoLoteActual;
                    totalProcesados += tamanoLoteActual;
                    offset += batchSize;

                    // Verificar si hay más datos
                    if (tamanoLoteActual < batchSize) {
                        log.debug("Lote incompleto ({} < {}), terminando provincia: {}",
                                tamanoLoteActual, batchSize, provincia);
                        hayMasDatos = false;
                    }

                    // Liberación EXPLÍCITA e INMEDIATA del lote
                    liberarLoteCompletamente(lote);

                    // Gestión de memoria periódica
                    gestionarMemoriaPeriodica(offset, batchSize, provincia);

                    // Log de progreso cada 10 lotes
                    if ((offset / batchSize) % 10 == 0) {
                        log.info("Progreso provincia {}: {} registros procesados, memoria: {}%",
                                provincia, procesadosEnProvincia, obtenerPorcentajeMemoriaUsada());
                    }

                } catch (OutOfMemoryError oom) {
                    log.error("OUT OF MEMORY en provincia {}, offset {}: {}", provincia, offset, oom.getMessage());
                    // Intentar recuperación
                    limpiarMemoriaAgresiva();
                    break; // Salir del while para esta provincia

                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, offset {}: {}",
                            provincia, offset, e.getMessage(), e);

                    // Intentar continuar con siguiente lote si el error no es crítico
                    if (e.getMessage().contains("memoria") || e.getMessage().contains("memory")) {
                        limpiarMemoria();
                        offset += batchSize; // Saltar este lote problemático
                    } else {
                        break; // Salir del while para errores no relacionados con memoria
                    }
                }
            }

            // Verificación post-provincia
            if (esMemoriaCritica()) {
                log.warn("Memoria crítica tras completar provincia: {}. Esperando liberación...", provincia);
                esperarLiberacionMemoria();
            }

            log.info("Completada provincia {}: {} registros procesados, memoria final: {}%",
                    provincia, procesadosEnProvincia, obtenerPorcentajeMemoriaUsada());

            // Limpieza entre provincias
            if (repositorioActual < totalRepositorios) {
                pausaEntreProvincia();
            }
        }

        log.info("Procesamiento en lotes completado. Total procesados: {}, memoria final: {}%",
                totalProcesados, obtenerPorcentajeMemoriaUsada());
    }

    /**
     * Procesa el lote en chunks con liberación inmediata de memoria
     */
    private void procesarLoteEnChunksConLiberacion(List<Map<String, Object>> lote,
                                                   Consumer<List<Map<String, Object>>> procesadorLote) {

        int currentChunkSize = Math.min(chunkSize, lote.size());
        int chunksProcessed = 0;

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());

            // Crear sublista (vista, no copia completa)
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            try {
                // Procesar el chunk
                procesadorLote.accept(chunk);
                chunksProcessed++;

                log.trace("Chunk {}/{} procesado, tamaño: {}",
                        chunksProcessed, (lote.size() + currentChunkSize - 1) / currentChunkSize, chunk.size());

            } catch (Exception e) {
                log.error("Error procesando chunk {}: {}", chunksProcessed, e.getMessage());
                throw e; // Re-lanzar para manejo en nivel superior
            }

            // Liberación EXPLÍCITA del chunk
            chunk.clear(); // Aunque sea una vista, limpiar referencias
            chunk = null;

            // Gestión de memoria cada 5 chunks
            if (chunksProcessed % 5 == 0) {
                if (esMemoriaAlta()) {
                    pausaMicro();
                }

                // Limpieza más agresiva cada 10 chunks
                if (chunksProcessed % 10 == 0) {
                    limpiarMemoria();

                    log.trace("Limpieza post-chunk {}, memoria: {}%",
                            chunksProcessed, obtenerPorcentajeMemoriaUsada());
                }
            }
        }
    }

    /**
     * Agrega provincia a registros en chunks para evitar overhead
     */
    private void procesarProvinciaEnChunks(List<Map<String, Object>> lote, String provincia) {
        int chunkSize = Math.min(100, lote.size()); // Chunks pequeños para esto

        for (int i = 0; i < lote.size(); i += chunkSize) {
            int endIndex = Math.min(i + chunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                lote.get(j).put("provincia", provincia);
            }

            // Micro-pausa cada chunk para permitir GC
            if (i > 0 && (i / chunkSize) % 20 == 0) {
                Thread.yield();
            }
        }
    }

    /**
     * Liberación completa y agresiva del lote
     */
    private void liberarLoteCompletamente(List<Map<String, Object>> lote) {
        try {
            // Limpiar cada mapa individual
            for (Map<String, Object> registro : lote) {
                if (registro != null) {
                    registro.clear();
                }
            }

            // Limpiar la lista
            lote.clear();

            // Anular referencia
            lote = null;

        } catch (Exception e) {
            log.warn("Error liberando lote: {}", e.getMessage());
        }
    }

    /**
     * Gestión de memoria periódica durante procesamiento
     */
    private void gestionarMemoriaPeriodica(int offset, int batchSize, String provincia) {
        // Cada 5 lotes
        if (offset % (batchSize * 5) == 0) {
            pausaInteligente();

            // Log de memoria cada 5 lotes
            log.debug("Memoria tras {} registros en {}: {}%",
                    offset, provincia, obtenerPorcentajeMemoriaUsada());
        }

        // Limpieza más agresiva cada 20 lotes
        if (offset % (batchSize * 20) == 0) {
            limpiarMemoria();
            log.debug("Limpieza agresiva en provincia {}, offset: {}", provincia, offset);
        }
    }

    /**
     * Pausa entre provincias para estabilizar memoria
     */
    private void pausaEntreProvincia() {
        try {
            log.debug("Pausa entre provincias, memoria antes: {}%", obtenerPorcentajeMemoriaUsada());

            // Limpieza pre-pausa
            limpiarMemoria();

            // Pausa para permitir estabilización
            Thread.sleep(100);

            log.debug("Memoria después de pausa: {}%", obtenerPorcentajeMemoriaUsada());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Limpieza agresiva de memoria para situaciones críticas
     */
    private void limpiarMemoriaAgresiva() {
        log.warn("Ejecutando limpieza agresiva de memoria...");

        // Múltiples pasadas de GC
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.warn("Limpieza agresiva completada. Memoria actual: {}%", obtenerPorcentajeMemoriaUsada());
    }

    /**
     * Obtiene porcentaje de memoria usada para logs
     */
    private double obtenerPorcentajeMemoriaUsada() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        return (double) memoriaUsada / runtime.totalMemory() * 100;
    }

    /**
     * Divide un lote en chunks más pequeños y los procesa uno por uno
     */
    private void procesarLoteEnChunks(List<Map<String, Object>> lote,
                                      Consumer<List<Map<String, Object>>> procesadorLote) {

        int currentChunkSize = Math.min(chunkSize, lote.size());

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());

            // Crear sublista (vista, no copia completa)
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            // Procesar el chunk
            procesadorLote.accept(chunk);


            // Liberar referencia explícitamente
            chunk = null;

            // Verificar memoria cada 10 chunks
            if (i > 0 && (i / currentChunkSize) % 10 == 0 && esMemoriaAlta()) {
                pausaMicro();
            }
        }
    }

    /**
     * Verifica si la memoria está en nivel crítico
     */
    private boolean esMemoriaCritica() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > memoryCriticalThreshold;
    }

    /**
     * Verifica si la memoria está alta (pero no crítica)
     */
    private boolean esMemoriaAlta() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > 0.70;
    }

    /**
     * Pausa inteligente que permite GC natural sin forzarlo
     */
    private void pausaInteligente() {
        try {
            // Pausa muy corta para permitir que el GC natural actúe
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Pausa microscópica para chunks
     */
    private void pausaMicro() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Espera activa cuando la memoria está crítica
     */
    private void esperarLiberacionMemoria() {
        int intentos = 0;
        while (esMemoriaCritica() && intentos < 10) {
            try {
                log.warn("Memoria crítica detectada, esperando liberación... (intento {})", intentos + 1);
                Thread.sleep(1000); // Espera 1 segundo
                intentos++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }



  /**
          * Calcula el tamaño óptimo de lote usando el nuevo método
 */
    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
        // Usar el límite efectivo del DTO actualizado
        int batchSizeBase = filtros.getLimiteEfectivo();

        // Si es muy pequeño, usar el defaultBatchSize
        if (batchSizeBase < 1000) {
            batchSizeBase = defaultBatchSize;
        }

        log.info("Límite base calculado: {} (info: {})",
                batchSizeBase, filtros.getInfoPaginacion());

        // Considerar memoria disponible
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaTotal = runtime.totalMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaTotal;

        int batchSizeOptimo = batchSizeBase;

        // Ajustar según memoria disponible
        if (porcentajeLibre < 0.20) {
            batchSizeOptimo = Math.max(1000, batchSizeBase / 4);
            log.warn("Memoria muy baja ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        } else if (porcentajeLibre < 0.30) {
            batchSizeOptimo = Math.max(2000, batchSizeBase / 2);
            log.info("Memoria baja ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        } else if (porcentajeLibre > 0.70) {
            // Memoria abundante, mantener o aumentar
            batchSizeOptimo = Math.min(batchSizeBase, 10000); // Máximo 10k
            log.info("Memoria abundante ({}%), usando lote de {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        }

        log.info("Tamaño de lote FINAL: {} (base: {}, memoria: {}%)",
                batchSizeOptimo, batchSizeBase, String.format("%.1f", porcentajeLibre * 100));

        return batchSizeOptimo;
    }


    /**
     * Crea filtros para lote usando el DTO actualizado
     */
    private ParametrosFiltrosDTO crearFiltrosParaLote(ParametrosFiltrosDTO filtrosOriginales,
                                                      int batchSize, int offset) {

        log.debug("Creando filtros - batchSize: {}, offset: {}, original: {}",
                batchSize, offset, filtrosOriginales.getInfoPaginacion());

        ParametrosFiltrosDTO filtrosLote = filtrosOriginales.toBuilder()
                .limite(batchSize)           // Establecer límite explícito
                .offset(offset)              // Establecer offset explícito
                .pagina(null)                // Limpiar página para evitar conflictos
                .tamanoPagina(null)          // Limpiar tamaño página
                .build();

        // Validar que se creó correctamente
        if (!filtrosLote.validarPaginacion()) {
            log.warn("Filtros de lote inválidos: {}", filtrosLote.getInfoPaginacion());
        }

        log.debug("Filtros de lote creados: {}", filtrosLote.getInfoPaginacion());

        return filtrosLote;
    }

    /**
     * Estimación del uso de memoria por registro (aproximado)
     */
    private long estimarMemoriaPorRegistro(Map<String, Object> registro) {
        // Estimación aproximada: cada campo como 50 bytes promedio
        return registro.size() * 50L;
    }

    /**
     * Verifica si hay suficiente memoria para procesar un lote
     */
    public boolean hayMemoriaSuficiente(int tamanoLote) {
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaEstimada = tamanoLote * 1024L; // 1KB por registro estimado

        return memoriaLibre > (memoriaEstimada * 2); // Factor de seguridad x2
    }

    /**
     * Fuerza limpieza de memoria
     */
    public void limpiarMemoria() {
        // Solo sugerir GC si realmente es necesario
        if (esMemoriaCritica()) {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            // Solo una pausa pequeña para permitir GC natural
            pausaInteligente();
        }
    }

    /**
     * Obtiene estadísticas de memoria actual
     */
    public Map<String, Object> obtenerEstadisticasMemoria() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaTotal = runtime.totalMemory();
        long memoriaLibre = runtime.freeMemory();
        long memoriaUsada = memoriaTotal - memoriaLibre;
        long memoriaMaxima = runtime.maxMemory();

        Map<String, Object> stats = new HashMap<>();
        stats.put("memoria_total_mb", memoriaTotal / (1024 * 1024));
        stats.put("memoria_libre_mb", memoriaLibre / (1024 * 1024));
        stats.put("memoria_usada_mb", memoriaUsada / (1024 * 1024));
        stats.put("memoria_maxima_mb", memoriaMaxima / (1024 * 1024));
        stats.put("porcentaje_uso", (double) memoriaUsada / memoriaTotal * 100);

        return stats;
    }

    /**
     * Obtiene estadísticas extendidas incluyendo chunks
     */
    public Map<String, Object> obtenerEstadisticasMemoriaExtendidas() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaTotal = runtime.totalMemory();
        long memoriaLibre = runtime.freeMemory();
        long memoriaUsada = memoriaTotal - memoriaLibre;
        long memoriaMaxima = runtime.maxMemory();

        Map<String, Object> stats = new HashMap<>();
        stats.put("memoria_total_mb", memoriaTotal / (1024 * 1024));
        stats.put("memoria_libre_mb", memoriaLibre / (1024 * 1024));
        stats.put("memoria_usada_mb", memoriaUsada / (1024 * 1024));
        stats.put("memoria_maxima_mb", memoriaMaxima / (1024 * 1024));
        stats.put("porcentaje_uso", (double) memoriaUsada / memoriaTotal * 100);

        // Nuevas estadísticas
        stats.put("chunk_size_configurado", chunkSize);
        stats.put("memoria_critica_threshold", memoryCriticalThreshold);
        stats.put("es_memoria_critica", esMemoriaCritica());
        stats.put("es_memoria_alta", esMemoriaAlta());

        return stats;
    }

}