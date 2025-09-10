package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.cursor.default-page-size:500}")
    private int defaultCursorPageSize;

    @Value("${app.cursor.max-page-size:1000}")
    private int maxCursorPageSize;

    @Value("${app.cursor.default-type:fecha_id}")
    private String defaultCursorType;

    @Value("${app.batch.chunk-size:250}")
    private int chunkSize;

    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double memoryCriticalThreshold;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    /**
     * Procesa datos usando CURSOR en lugar de OFFSET (mucho más eficiente)
     */
    public void procesarEnLotesConCursor(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtrosBase,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("Iniciando procesamiento con CURSOR para {} repositorios", repositories.size());

        int pageSize = calcularPageSizeOptimo(filtrosBase);
        int totalProcesados = 0;
        int totalRepositorios = repositories.size();
        int repositorioActual = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            repositorioActual++;
            String provincia = repo.getProvincia();
            log.info("Procesando provincia {}/{}: {}", repositorioActual, totalRepositorios, provincia);

            String cursor = null; // Empezar desde primera página
            boolean hayMasDatos = true;
            int procesadosEnProvincia = 0;
            int iteraciones = 0;
            final int maxIteracionesPorProvincia = 1000; // Límite de seguridad

            while (hayMasDatos && !esMemoriaCritica() && iteraciones < maxIteracionesPorProvincia) {
                try {
                    // Verificar memoria ANTES de procesar
                    if (esMemoriaAlta()) {
                        log.warn("Memoria alta detectada antes de procesar lote. Provincia: {}, Iteración: {}",
                                provincia, iteraciones);
                        pausaInteligente();
                    }

                    // Crear filtros para este lote con cursor
                    ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLoteConCursor(
                            filtrosBase, cursor, pageSize);

                    // Ejecutar consulta para este lote
                    List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                    if (lote == null || lote.isEmpty()) {
                        log.debug("No hay más datos en provincia: {}, cursor: {}", provincia, cursor);
                        hayMasDatos = false;
                        break;
                    }

                    int tamanoLoteActual = lote.size();

                    // Verificar si hay más datos (lote viene con +1 registro)
                    boolean hayMasEnEstaPagina = tamanoLoteActual > pageSize;
                    List<Map<String, Object>> loteParaProcesar = hayMasEnEstaPagina ?
                            lote.subList(0, pageSize) : lote;

                    // Agregar información de provincia a cada registro
                    procesarProvinciaEnChunks(loteParaProcesar, provincia);

                    log.debug("Procesando lote: provincia={}, cursor={}, tamaño={}, memoria={}%",
                            provincia, cursor, loteParaProcesar.size(), obtenerPorcentajeMemoriaUsada());

                    // Procesar el lote en chunks pequeños con liberación frecuente
                    procesarLoteEnChunksConLiberacion(loteParaProcesar, procesadorLote);

                    // Actualizar contadores
                    procesadosEnProvincia += loteParaProcesar.size();
                    totalProcesados += loteParaProcesar.size();
                    iteraciones++;

                    // Extraer cursor para siguiente página
                    if (hayMasEnEstaPagina) {
                        cursor = ParametrosFiltrosDTO.extraerCursorDeResultados(
                                loteParaProcesar, filtrosLote.getTipoCursor());
                        log.debug("Cursor extraído para siguiente página: {}", cursor);
                    } else {
                        hayMasDatos = false;
                        log.debug("Última página procesada en provincia: {}", provincia);
                    }

                    // Liberación EXPLÍCITA e INMEDIATA del lote
                    liberarLoteCompletamente(lote);
                    liberarLoteCompletamente(loteParaProcesar);

                    // Gestión de memoria periódica
                    gestionarMemoriaPeriodica(iteraciones, provincia);

                    // Log de progreso cada 10 iteraciones
                    if (iteraciones % 10 == 0) {
                        log.info("Progreso provincia {}: {} registros procesados, {} iteraciones, memoria: {}%",
                                provincia, procesadosEnProvincia, iteraciones, obtenerPorcentajeMemoriaUsada());
                    }

                } catch (OutOfMemoryError oom) {
                    log.error("OUT OF MEMORY en provincia {}, iteración {}: {}",
                            provincia, iteraciones, oom.getMessage());
                    limpiarMemoriaAgresiva();
                    break; // Salir del while para esta provincia

                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, iteración {}: {}",
                            provincia, iteraciones, e.getMessage(), e);

                    // Para cursor, intentar continuar con siguiente página
                    if (e.getMessage().contains("memoria") || e.getMessage().contains("memory")) {
                        limpiarMemoria();
                        // Con cursor, no "saltamos" - simplemente continuamos al siguiente
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

            if (iteraciones >= maxIteracionesPorProvincia) {
                log.warn("Alcanzado límite máximo de iteraciones ({}) en provincia: {}",
                        maxIteracionesPorProvincia, provincia);
            }

            log.info("Completada provincia {}: {} registros procesados, {} iteraciones, memoria final: {}%",
                    provincia, procesadosEnProvincia, iteraciones, obtenerPorcentajeMemoriaUsada());

            // Limpieza entre provincias
            if (repositorioActual < totalRepositorios) {
                pausaEntreProvincia();
            }
        }

        log.info("Procesamiento con cursor completado. Total procesados: {}, memoria final: {}%",
                totalProcesados, obtenerPorcentajeMemoriaUsada());
    }

    /**
     * Calcula pageSize óptimo para cursor (reemplaza calcularTamanoLoteOptimo)
     */
    private int calcularPageSizeOptimo(ParametrosFiltrosDTO filtros) {
        int pageSizeBase = filtros != null ? filtros.getPageSizeEfectivo() : defaultCursorPageSize;

        // Si es muy pequeño para batch, usar un tamaño más grande
        if (pageSizeBase < 100) {
            pageSizeBase = defaultCursorPageSize;
        }

        log.info("PageSize base calculado: {} (info: {})",
                pageSizeBase, filtros != null ? filtros.getInfoPaginacion() : "filtros null");

        // Considerar memoria disponible
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaTotal = runtime.totalMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaTotal;

        int pageSizeOptimo = pageSizeBase;

        // Ajustar según memoria disponible
        if (porcentajeLibre < 0.20) {
            pageSizeOptimo = Math.max(100, pageSizeBase / 4);
            log.warn("Memoria muy baja ({}%), reduciendo pageSize a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), pageSizeOptimo);
        } else if (porcentajeLibre < 0.30) {
            pageSizeOptimo = Math.max(200, pageSizeBase / 2);
            log.info("Memoria baja ({}%), reduciendo pageSize a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), pageSizeOptimo);
        } else if (porcentajeLibre > 0.70) {
            // Memoria abundante, mantener o aumentar (pero respetar límite)
            pageSizeOptimo = Math.min(pageSizeBase * 2, maxCursorPageSize);
            log.info("Memoria abundante ({}%), usando pageSize de {}",
                    String.format("%.1f%%", porcentajeLibre * 100), pageSizeOptimo);
        }

        log.info("PageSize FINAL: {} (base: {}, memoria: {}%)",
                pageSizeOptimo, pageSizeBase, String.format("%.1f", porcentajeLibre * 100));

        return pageSizeOptimo;
    }

    /**
     * Crea filtros para lote con cursor (reemplaza crearFiltrosParaLote)
     */
    private ParametrosFiltrosDTO crearFiltrosParaLoteConCursor(
            ParametrosFiltrosDTO filtrosOriginales, String cursor, int pageSize) {

        log.debug("Creando filtros con cursor - pageSize: {}, cursor: {}",
                pageSize, cursor != null ? cursor.substring(0, Math.min(cursor.length(), 20)) + "..." : "null");

        // Crear filtros base para cursor
        ParametrosFiltrosDTO filtrosLote;

        if (cursor == null) {
            // Primera página
            filtrosLote = ParametrosFiltrosDTO.primeraPagina(pageSize, defaultCursorType);
        } else {
            // Páginas siguientes
            filtrosLote = ParametrosFiltrosDTO.siguientePagina(cursor, defaultCursorType, pageSize);
        }

        // Combinar con filtros originales si existen
        if (filtrosOriginales != null) {
            filtrosLote = combinarFiltros(filtrosLote, filtrosOriginales);
        }

        // Validar que se creó correctamente
        if (!filtrosLote.validarPaginacion()) {
            log.warn("Filtros de lote con cursor inválidos: {}", filtrosLote.getInfoPaginacion());
        }

        log.debug("Filtros de lote con cursor creados: {}", filtrosLote.getInfoPaginacion());

        return filtrosLote;
    }

    /**
     * Combina filtros de cursor con filtros originales de búsqueda
     */
    private ParametrosFiltrosDTO combinarFiltros(ParametrosFiltrosDTO filtrosCursor,
                                                 ParametrosFiltrosDTO filtrosOriginales) {
        return filtrosCursor.toBuilder()
                // Mantener filtros de búsqueda originales
                .fechaInicio(filtrosOriginales.getFechaInicio())
                .fechaFin(filtrosOriginales.getFechaFin())
                .fechaEspecifica(filtrosOriginales.getFechaEspecifica())
                .provincias(filtrosOriginales.getProvincias())
                .municipios(filtrosOriginales.getMunicipios())
                .lugares(filtrosOriginales.getLugares())
                .partido(filtrosOriginales.getPartido())
                .baseDatos(filtrosOriginales.getBaseDatos())
                .patronesEquipos(filtrosOriginales.getPatronesEquipos())
                .tiposDispositivos(filtrosOriginales.getTiposDispositivos())
                .seriesEquiposExactas(filtrosOriginales.getSeriesEquiposExactas())
                .concesiones(filtrosOriginales.getConcesiones())
                .tiposInfracciones(filtrosOriginales.getTiposInfracciones())
                .estadosInfracciones(filtrosOriginales.getEstadosInfracciones())
                .exportadoSacit(filtrosOriginales.getExportadoSacit())
                .tipoVehiculo(filtrosOriginales.getTipoVehiculo())
                .tieneEmail(filtrosOriginales.getTieneEmail())
                .tipoDocumento(filtrosOriginales.getTipoDocumento())
                .usarTodasLasBDS(filtrosOriginales.getUsarTodasLasBDS())
                .filtrosAdicionales(filtrosOriginales.getFiltrosAdicionales())
                .build();
    }

    // =============== MÉTODOS EXISTENTES (SIN CAMBIOS) ===============

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
        int chunkSizeProvincia = Math.min(100, lote.size()); // Chunks pequeños para esto

        for (int i = 0; i < lote.size(); i += chunkSizeProvincia) {
            int endIndex = Math.min(i + chunkSizeProvincia, lote.size());

            for (int j = i; j < endIndex; j++) {
                lote.get(j).put("provincia_origen", provincia);
            }

            // Micro-pausa cada chunk para permitir GC
            if (i > 0 && (i / chunkSizeProvincia) % 20 == 0) {
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
     * Gestión de memoria periódica durante procesamiento (actualizada para iteraciones)
     */
    private void gestionarMemoriaPeriodica(int iteracion, String provincia) {
        // Cada 5 iteraciones
        if (iteracion % 5 == 0) {
            pausaInteligente();

            // Log de memoria cada 5 iteraciones
            log.debug("Memoria tras {} iteraciones en {}: {}%",
                    iteracion, provincia, obtenerPorcentajeMemoriaUsada());
        }

        // Limpieza más agresiva cada 20 iteraciones
        if (iteracion % 20 == 0) {
            limpiarMemoria();
            log.debug("Limpieza agresiva en provincia {}, iteración: {}", provincia, iteracion);
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
     * Obtiene estadísticas extendidas incluyendo configuración de cursor
     */
    public Map<String, Object> obtenerEstadisticasExtendidas() {
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

        // Estadísticas de cursor
        stats.put("cursor_page_size_default", defaultCursorPageSize);
        stats.put("cursor_page_size_max", maxCursorPageSize);
        stats.put("cursor_type_default", defaultCursorType);
        stats.put("chunk_size_configurado", chunkSize);
        stats.put("memoria_critica_threshold", memoryCriticalThreshold);
        stats.put("es_memoria_critica", esMemoriaCritica());
        stats.put("es_memoria_alta", esMemoriaAlta());

        return stats;
    }

    // =============== MÉTODO PÚBLICO PARA COMPATIBILIDAD ===============

    /**
     * Método público para compatibilidad (mantiene la interfaz existente pero usa cursor)
     */
    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("Método legacy procesarEnLotes llamado - redirigiendo a cursor");
        procesarEnLotesConCursor(repositories, filtros, nombreQuery, procesadorLote);
    }
}