package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.batch.size:5000}")
    private int defaultBatchSize;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryPerBatchMB;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${app.batch.chunk-size:250}")
    private int chunkSize;

    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double memoryCriticalThreshold;

    @Value("${app.batch.parallel-threshold-per-province:50000}")
    private int parallelThresholdPerProvince;

    @Value("${app.batch.parallel-threshold-total:300000}")
    private int parallelThresholdTotal;

    @Value("${app.batch.massive-threshold-per-province:200000}")
    private int massiveThresholdPerProvince;

    @Value("${app.batch.max-parallel-provinces:6}")
    private int maxParallelProvinces;

    private final Executor parallelExecutor = Executors.newFixedThreadPool(6);
    private Consumer<List<Map<String, Object>>> procesadorLoteGlobal;
    private BufferedWriter archivoSalida;
    private Queue<Map<String, Object>> colaResultados = new ConcurrentLinkedQueue<>();
    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();

    // Campos para tracking de progreso
    private long tiempoInicioGlobal;
    private int totalRepositoriosProcesados = 0;
    private int totalRegistrosGlobales = 0;
    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();
    private long ultimoHeartbeat = 0;
    private static final long HEARTBEAT_INTERVAL_MS = 30000; // 30 segundos

    public void configurarProcesadorInmediato(Consumer<List<Map<String, Object>>> procesador) {
        this.procesadorLoteGlobal = procesador;
    }

    @AllArgsConstructor
    private static class EstimacionDataset {
        final int totalEstimado;
        final double promedioPorProvincia;
        final int maximoPorProvincia;
        final List<Integer> muestrasPorProvicia;
    }

    /**
     * Procesamiento datos en lotes con logs mejorados
     */
    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesarLotes
    ) {
        tiempoInicioGlobal = System.currentTimeMillis();
        ultimoHeartbeat = tiempoInicioGlobal;
        totalRepositoriosProcesados = 0;
        totalRegistrosGlobales = 0;
        contadoresPorProvincia.clear();

        log.info("═══════════════════════════════════════════════════════════");
        log.info("🚀 INICIO DEL PROCESAMIENTO");
        log.info("   Provincias a procesar: {}", repositories.size());
        log.info("   Query: {}", nombreQuery);
        log.info("   Memoria inicial: {:.1f}%", obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");

        // 1. Estimación
        EstimacionDataset estimacionDataset = estimarDataset(repositories, filtros, nombreQuery);

        log.info("📊 ESTIMACIÓN DEL DATASET:");
        log.info("   Total estimado: {} registros", estimacionDataset.totalEstimado);
        log.info("   Promedio por provincia: {} registros", (int) estimacionDataset.promedioPorProvincia);
        log.info("   Máximo por provincia: {} registros", estimacionDataset.maximoPorProvincia);

        // 2. Decisión Estrategia
        EstrategiaProcessing estrategia = decidirEstrategia(estimacionDataset);
        log.info("⚙️  ESTRATEGIA SELECCIONADA: {}", estrategia);
        log.info("═══════════════════════════════════════════════════════════");

        // 3. Ejecutar Estrategia
        switch (estrategia) {
            case PARALELO:
                procesarParaleloCompletoConStreaming(repositories, filtros, nombreQuery, procesarLotes);
                break;
            case HIBRIDO:
                procesarHibridoControlado(repositories, filtros, nombreQuery, procesarLotes, estimacionDataset);
                break;
            case SECUENCIAL:
                procesarEnLotesFormaSecuencial(repositories, filtros, nombreQuery, procesarLotes);
                break;
        }

        // Resumen final
        long duracionTotal = System.currentTimeMillis() - tiempoInicioGlobal;
        log.info("═══════════════════════════════════════════════════════════");
        log.info("✅ PROCESAMIENTO COMPLETADO");
        log.info("   Duración total: {} ms ({} segundos)", duracionTotal, duracionTotal / 1000);
        log.info("   Total procesado: {} registros", totalRegistrosGlobales);
        log.info("   Velocidad promedio: {} registros/segundo",
                totalRegistrosGlobales * 1000 / Math.max(duracionTotal, 1));
        log.info("   Memoria final: {:.1f}%", obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");

        // Resumen por provincia
        if (!contadoresPorProvincia.isEmpty()) {
            log.info("📋 RESUMEN POR PROVINCIA:");
            contadoresPorProvincia.entrySet().stream()
                    .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                    .forEach(entry ->
                            log.info("   {} → {} registros", entry.getKey(), entry.getValue())
                    );
        }
    }

    private EstimacionDataset estimarDataset(List<InfraccionesRepositoryImpl> repositories,
                                             ParametrosFiltrosDTO filtros,
                                             String nombreQuery) {
        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(50)
                .offset(0)
                .build();

        List<Integer> muestras = repositories.parallelStream()
                .map(repo -> {
                    try {
                        List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);
                        if (muestra.size() >= 50) {
                            return 10000;
                        } else if (muestra.size() >= 20) {
                            return muestra.size() * 500;
                        } else if (muestra.size() > 0) {
                            return muestra.size() * 200;
                        }
                        return 0;
                    } catch (Exception e) {
                        log.warn("Error estimando para provincia {}: {}", repo.getProvincia(), e.getMessage());
                        return 1000;
                    }
                })
                .collect(Collectors.toList());

        int totalEstimado = muestras.stream().mapToInt(Integer::intValue).sum();
        int promedioPorProvincia = totalEstimado / repositories.size();
        int maximoPorProvincia = muestras.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedioPorProvincia, maximoPorProvincia, muestras);
    }

    private EstrategiaProcessing decidirEstrategia(EstimacionDataset estimacion) {
        if (estimacion.promedioPorProvincia < parallelThresholdPerProvince &&
                estimacion.totalEstimado < parallelThresholdTotal) {
            return EstrategiaProcessing.PARALELO;
        }
        if (estimacion.maximoPorProvincia > massiveThresholdPerProvince) {
            return EstrategiaProcessing.SECUENCIAL;
        }
        return EstrategiaProcessing.HIBRIDO;
    }

    private void procesarParaleloCompletoConStreaming(List<InfraccionesRepositoryImpl> repositories,
                                                      ParametrosFiltrosDTO filtros,
                                                      String nombreQuery,
                                                      Consumer<List<Map<String, Object>>> procesadorLote) {
        log.info("🚀 Ejecutando PARALELO COMPLETO CON STREAMING");
        long startTime = System.currentTimeMillis();

        this.procesadorLoteGlobal = procesadorLote;

        List<CompletableFuture<Void>> futures = repositories.stream()
                .map(repo -> CompletableFuture.runAsync(() -> {
                    ejecutarProvinciaCompleta(repo, filtros, nombreQuery);
                }, parallelExecutor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        finalizarProcesamiento();

        long duration = System.currentTimeMillis() - startTime;
        log.info("✅ PARALELO COMPLETO CON STREAMING finalizado en {} ms", duration);
    }

    private void procesarHibridoControlado(List<InfraccionesRepositoryImpl> repositories,
                                           ParametrosFiltrosDTO filtros,
                                           String nombreQuery,
                                           Consumer<List<Map<String, Object>>> procesadorLote,
                                           EstimacionDataset estimacion) {
        log.info("⚖️ Ejecutando HÍBRIDO CONTROLADO con {} provincias paralelas", maxParallelProvinces);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
            List<InfraccionesRepositoryImpl> grupoRepositories = repositories.subList(i, endIndex);

            log.info("📦 Procesando grupo {}-{} de {} provincias", i + 1, endIndex, repositories.size());

            List<CompletableFuture<List<Map<String, Object>>>> futures = grupoRepositories.stream()
                    .map(repo -> CompletableFuture.supplyAsync(() -> {
                        return ejecutarProvinciaCompleta(repo, filtros, nombreQuery);
                    }, parallelExecutor))
                    .collect(Collectors.toList());

            futures.forEach(future -> {
                try {
                    List<Map<String, Object>> datos = future.get();
                    if (!datos.isEmpty()) {
                        procesadorLote.accept(datos);
                    }
                } catch (Exception e) {
                    log.error("Error en grupo híbrido: {}", e.getMessage());
                }
            });

            if (endIndex < repositories.size()) {
                try {
                    Thread.sleep(500);
                    System.gc();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("✅ HÍBRIDO CONTROLADO finalizado en {} ms", duration);
    }

    public void procesarEnLotesFormaSecuencial(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        int batchSize = calcularTamanoLoteOptimo(filtros);
        int totalProcesados = 0;
        int totalRepositorios = repositories.size();
        int repositorioActual = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            repositorioActual++;
            String provincia = repo.getProvincia();
            long tiempoInicioProvincia = System.currentTimeMillis();

            log.info("┌─────────────────────────────────────────────────────────┐");
            log.info("│ PROVINCIA [{}/{}]: {}", repositorioActual, totalRepositorios, provincia);
            log.info("│ Memoria actual: {:.1f}%", obtenerPorcentajeMemoriaUsada());
            log.info("│ Procesados hasta ahora: {} registros", totalProcesados);
            log.info("└─────────────────────────────────────────────────────────┘");

            lastKeyPerProvince.remove(provincia);

            int offset = 0;
            boolean hayMasDatos = true;
            int procesadosEnProvincia = 0;

            while (hayMasDatos) {
                try {
                    logHeartbeatSiCorresponde(totalRepositorios);

                    if (esMemoriaCritica()) {
                        log.warn("Memoria crítica detectada ({:.1f}%) - Reduciendo batch size y limpiando",
                                obtenerPorcentajeMemoriaUsada());
                        batchSize = Math.max(500, batchSize / 2);
                        limpiarMemoriaAgresiva();
                        if (esMemoriaCritica()) {
                            log.warn("Memoria sigue crítica, pero continuando con batch pequeño: {}", batchSize);
                        }
                    }

                    if (esMemoriaAlta()) {
                        log.warn("Memoria alta detectada antes de procesar lote. Provincia: {}, Offset: {}",
                                provincia, offset);
                        pausaInteligente();
                    }

                    ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset, provincia);
                    List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                    if (lote == null || lote.isEmpty()) {
                        log.debug("No hay más datos en provincia: {}, offset: {}", provincia, offset);
                        hayMasDatos = false;
                        break;
                    }

                    int tamanoLoteActual = lote.size();

                    if (!lote.isEmpty()) {
                        Map<String, Object> ultimoRegistro = lote.get(lote.size() - 1);
                        Object[] lastKey = new Object[]{
                                ultimoRegistro.get("id"),
                                ultimoRegistro.get("serie_equipo"),
                                ultimoRegistro.get("lugar")
                        };
                        lastKeyPerProvince.put(provincia, lastKey);
                    }

                    procesarProvinciaEnChunks(lote, provincia);
                    log.debug("Procesando lote: provincia={}, offset={}, tamaño={}, memoria={:.1f}%",
                            provincia, offset, tamanoLoteActual, obtenerPorcentajeMemoriaUsada());

                    procesarLoteEnChunksConLiberacion(lote, procesadorLote);

                    procesadosEnProvincia += tamanoLoteActual;
                    totalProcesados += tamanoLoteActual;
                    totalRegistrosGlobales += tamanoLoteActual;
                    offset += batchSize;

                    if (tamanoLoteActual < batchSize) {
                        log.debug("Lote incompleto ({} < {}), terminando provincia: {}",
                                tamanoLoteActual, batchSize, provincia);
                        hayMasDatos = false;
                    }

                    liberarLoteCompletamente(lote);
                    gestionarMemoriaPeriodica(offset, batchSize, provincia);

                    if ((offset / batchSize) % 10 == 0 && offset > 0) {
                        log.info("│ ⏳ Progreso {}: {} lotes procesados ({} registros) - Memoria: {:.1f}%",
                                provincia, offset / batchSize, procesadosEnProvincia, obtenerPorcentajeMemoriaUsada());
                    }

                } catch (OutOfMemoryError oom) {
                    log.error("OUT OF MEMORY en provincia {}, offset {}: {}", provincia, offset, oom.getMessage());
                    limpiarMemoriaAgresiva();
                    break;
                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, offset {}: {}",
                            provincia, offset, e.getMessage(), e);
                    if (e.getMessage().contains("memoria") || e.getMessage().contains("memory")) {
                        limpiarMemoria();
                        offset += batchSize;
                    } else {
                        break;
                    }
                }
            }

            long duracionProvincia = System.currentTimeMillis() - tiempoInicioProvincia;
            totalRepositoriosProcesados++;
            contadoresPorProvincia.put(provincia, procesadosEnProvincia);

            double progreso = (double) repositorioActual / totalRepositorios * 100;
            long tiempoTranscurrido = System.currentTimeMillis() - tiempoInicioGlobal;
            long tiempoEstimadoTotal = (long) (tiempoTranscurrido / progreso * 100);
            long tiempoRestante = tiempoEstimadoTotal - tiempoTranscurrido;

            log.info("┌─────────────────────────────────────────────────────────┐");
            log.info("│ ✅ PROVINCIA COMPLETADA: {}", provincia);
            log.info("│    Registros: {}", procesadosEnProvincia);
            log.info("│    Duración: {} ms ({} seg)", duracionProvincia, duracionProvincia / 1000);
            log.info("│    Velocidad: {} reg/seg",
                    procesadosEnProvincia * 1000 / Math.max(duracionProvincia, 1));
            log.info("│");
            log.info("│ 📈 PROGRESO GLOBAL:");
            log.info("│    Provincias: {}/{} ({:.1f}%)", repositorioActual, totalRepositorios, progreso);
            log.info("│    Total procesado: {} registros", totalProcesados);
            log.info("│    Tiempo transcurrido: {} min {} seg",
                    tiempoTranscurrido / 60000, (tiempoTranscurrido / 1000) % 60);
            log.info("│    Tiempo estimado restante: {} min {} seg",
                    tiempoRestante / 60000, (tiempoRestante / 1000) % 60);
            log.info("│    Memoria: {:.1f}%", obtenerPorcentajeMemoriaUsada());
            log.info("└─────────────────────────────────────────────────────────┘");

            if (repositorioActual < totalRepositorios) {
                double memoriaAntes = obtenerPorcentajeMemoriaUsada();

                for (int i = 0; i < 5; i++) {
                    System.gc();
                    System.runFinalization();
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                limpiarConexionesBD();

                double memoriaDespues = obtenerPorcentajeMemoriaUsada();
                double memoriaLiberada = memoriaAntes - memoriaDespues;

                log.info("Limpieza entre provincias completada. Memoria liberada: {:.1f}%, Memoria actual: {:.1f}%",
                        memoriaLiberada, memoriaDespues);

                if (memoriaDespues > 80.0) {
                    log.warn("Memoria sigue alta ({:.1f}%), aplicando pausa extendida", memoriaDespues);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                lastKeyPerProvince.remove(provincia);
                pausaEntreProvincia();
            }
        }

        log.info("Procesamiento en lotes completado. Total procesados: {}, memoria final: {:.1f}%",
                totalProcesados, obtenerPorcentajeMemoriaUsada());
    }

    private void logHeartbeatSiCorresponde(int totalRepositorios) {
        long ahora = System.currentTimeMillis();
        if (ahora - ultimoHeartbeat > HEARTBEAT_INTERVAL_MS) {
            long tiempoTranscurrido = ahora - tiempoInicioGlobal;
            log.info(" HEARTBEAT - Tiempo: {}min {}seg | Procesados: {} registros | Memoria: {:.1f}% | Provincias: {}/{}",
                    tiempoTranscurrido / 60000,
                    (tiempoTranscurrido / 1000) % 60,
                    totalRegistrosGlobales,
                    obtenerPorcentajeMemoriaUsada(),
                    totalRepositoriosProcesados,
                    totalRepositorios);
            ultimoHeartbeat = ahora;
        }
    }

    private List<Map<String, Object>> ejecutarProvinciaCompleta(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        String provincia = repo.getProvincia();

        log.info("=== DEBUG CONSOLIDACIÓN ===");
        log.info("filtros es null? {}", filtros == null);
        if (filtros != null) {
            log.info("consolidado field: {}", filtros.getConsolidado());
            log.info("consolidacion list: {}", filtros.getConsolidacion());
            log.info("esConsolidado()? {}", filtros.esConsolidado());
        }
        log.info("===========================");

        if (filtros != null && filtros.esConsolidado()) {
            log.info("Query {} es consolidable - ejecutando UNA SOLA VEZ (sin paginación)", nombreQuery);

            try {
                List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (resultado != null && !resultado.isEmpty()) {
                    log.info("Query consolidable retornó {} grupos para provincia {}",
                            resultado.size(), provincia);

                    resultado.forEach(registro -> registro.put("provincia", provincia));
                    procesarLoteInmediato(resultado);

                    resultado.clear();
                    resultado = null;

                    if (resultado != null && resultado.size() > 5000) {
                        limpiarMemoria();
                    }
                }

                return Collections.emptyList();

            } catch (Exception e) {
                log.error("Error ejecutando query consolidable {}: {}", nombreQuery, e.getMessage());
                return Collections.emptyList();
            }
        }

        try {
            int offset = 0;
            boolean hayMasDatos = true;
            int maxLoteMemoria = 1000;
            int procesadosEnProvincia = 0;

            lastKeyPerProvince.remove(provincia);

            while (hayMasDatos) {
                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, maxLoteMemoria, offset, provincia);

                log.debug("🔄 {} - Ejecutando lote: limite={}, offset={}, lastId={}",
                        provincia, filtrosLote.getLimite(), filtrosLote.getOffset(), filtrosLote.getLastId());

                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    hayMasDatos = false;
                } else {
                    if (!lote.isEmpty()) {
                        Map<String, Object> ultimoRegistro = lote.get(lote.size() - 1);
                        Object[] lastKey = new Object[]{
                                ultimoRegistro.get("id"),
                                ultimoRegistro.get("serie_equipo"),
                                ultimoRegistro.get("lugar")
                        };
                        lastKeyPerProvince.put(provincia, lastKey);
                    }

                    lote.forEach(registro -> registro.put("provincia", provincia));
                    procesarLoteInmediato(lote);

                    procesadosEnProvincia += lote.size();
                    offset += maxLoteMemoria;

                    if (lote.size() < maxLoteMemoria) {
                        hayMasDatos = false;
                    }

                    lote.clear();
                }
            }

            contadoresPorProvincia.put(provincia, procesadosEnProvincia);
            totalRegistrosGlobales += procesadosEnProvincia;

            lastKeyPerProvince.remove(provincia);
            return Collections.emptyList();

        } catch (Exception e) {
            log.error("Error ejecutando provincia {}: {}", provincia, e.getMessage());
            lastKeyPerProvince.remove(provincia);
            return Collections.emptyList();
        }
    }

    private void procesarLoteInmediato(List<Map<String, Object>> lote) {
        if (lote == null || lote.isEmpty()) {
            return;
        }

        try {
            log.debug("Procesando lote inmediato de {} registros", lote.size());

            if (this.procesadorLoteGlobal != null) {
                this.procesadorLoteGlobal.accept(lote);
                return;
            }

            if (this.archivoSalida != null) {
                escribirLoteAArchivo(lote);
                return;
            }

            if (this.colaResultados != null) {
                this.colaResultados.addAll(lote);
                if (this.colaResultados.size() >= this.chunkSize) {
                    procesarColaResultados();
                }
                return;
            }

            log.warn("procesarLoteInmediato llamado pero no hay procesador configurado. {} registros perdidos",
                    lote.size());

        } catch (Exception e) {
            log.error("Error en procesarLoteInmediato: {}", e.getMessage(), e);
            throw new RuntimeException("Fallo en procesamiento inmediato", e);
        }
    }

    private void procesarLoteEnChunksConLiberacion(List<Map<String, Object>> lote,
                                                   Consumer<List<Map<String, Object>>> procesadorLote) {
        int currentChunkSize = Math.min(chunkSize, lote.size());
        int chunksProcessed = 0;

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            try {
                procesadorLote.accept(chunk);
                chunksProcessed++;
                log.trace("Chunk {}/{} procesado, tamaño: {}",
                        chunksProcessed, (lote.size() + currentChunkSize - 1) / currentChunkSize, chunk.size());
            } catch (Exception e) {
                log.error("Error procesando chunk {}: {}", chunksProcessed, e.getMessage());
                throw e;
            }

            chunk.clear();
            chunk = null;

            if (chunksProcessed % 5 == 0) {
                if (esMemoriaAlta()) {
                    pausaMicro();
                }
                if (chunksProcessed % 10 == 0) {
                    limpiarMemoria();
                    log.trace("Limpieza post-chunk {}, memoria: {:.1f}%",
                            chunksProcessed, obtenerPorcentajeMemoriaUsada());
                }
            }
        }
    }

    private void procesarProvinciaEnChunks(List<Map<String, Object>> lote, String provincia) {
        int chunkSize = Math.min(100, lote.size());

        for (int i = 0; i < lote.size(); i += chunkSize) {
            int endIndex = Math.min(i + chunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                lote.get(j).put("provincia", provincia);
            }

            if (i > 0 && (i / chunkSize) % 20 == 0) {
                Thread.yield();
            }
        }
    }

    private void liberarLoteCompletamente(List<Map<String, Object>> lote) {
        try {
            for (Map<String, Object> registro : lote) {
                if (registro != null) {
                    registro.clear();
                }
            }
            lote.clear();
            lote = null;
        } catch (Exception e) {
            log.warn("Error liberando lote: {}", e.getMessage());
        }
    }

    private void gestionarMemoriaPeriodica(int offset, int batchSize, String provincia) {
        if (offset % (batchSize * 5) == 0) {
            pausaInteligente();
            log.debug("Memoria tras {} registros en {}: {:.1f}%",
                    offset, provincia, obtenerPorcentajeMemoriaUsada());
        }

        if (offset % (batchSize * 20) == 0) {
            limpiarMemoria();
            log.debug("Limpieza agresiva en provincia {}, offset: {}", provincia, offset);
        }
    }

    private void pausaEntreProvincia() {
        try {
            log.debug("Pausa entre provincias, memoria antes: {:.1f}%", obtenerPorcentajeMemoriaUsada());
            limpiarMemoria();
            Thread.sleep(100);
            log.debug("Memoria después de pausa: {:.1f}%", obtenerPorcentajeMemoriaUsada());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void limpiarMemoriaAgresiva() {
        log.warn("Ejecutando limpieza SUPER agresiva de memoria...");
        long memoriaAntes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < 5; i++) {
            System.gc();
            System.runFinalization();
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.gc();
        Thread.yield();

        long memoriaDespues = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoriaLiberada = memoriaAntes - memoriaDespues;

        log.warn("Limpieza agresiva completada. Liberados: {}MB, Memoria actual: {:.1f}%",
                memoriaLiberada / 1024 / 1024, obtenerPorcentajeMemoriaUsada());
    }

    private double obtenerPorcentajeMemoriaUsada() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        return (double) memoriaUsada / runtime.totalMemory() * 100;
    }

    private boolean esMemoriaCritica() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > memoryCriticalThreshold;
    }

    private boolean esMemoriaAlta() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > 0.70;
    }

    private void pausaInteligente() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void pausaMicro() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void esperarLiberacionMemoria() {
        int intentos = 0;
        while (esMemoriaCritica() && intentos < 10) {
            try {
                log.warn("Memoria crítica detectada, esperando liberación... (intento {})", intentos + 1);
                Thread.sleep(1000);
                intentos++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
        int batchSizeBase = filtros.getLimiteEfectivo();

        if (batchSizeBase < 1000) {
            batchSizeBase = defaultBatchSize;
        }

        log.info("Límite base calculado: {} (info: {})", batchSizeBase, filtros.getInfoPaginacion());

        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaTotal = runtime.totalMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaTotal;

        int batchSizeOptimo = batchSizeBase;

        if (porcentajeLibre < 0.20) {
            batchSizeOptimo = Math.max(1000, batchSizeBase / 4);
            log.warn("Memoria muy baja ({:.1f}%), reduciendo lote a {}", porcentajeLibre * 100, batchSizeOptimo);
        } else if (porcentajeLibre < 0.30) {
            batchSizeOptimo = Math.max(2000, batchSizeBase / 2);
            log.info("Memoria baja ({:.1f}%), reduciendo lote a {}", porcentajeLibre * 100, batchSizeOptimo);
        } else if (porcentajeLibre > 0.70) {
            batchSizeOptimo = Math.min(batchSizeBase, 10000);
            log.info("Memoria abundante ({:.1f}%), usando lote de {}", porcentajeLibre * 100, batchSizeOptimo);
        }

        log.info("Tamaño de lote FINAL: {} (base: {}, memoria: {:.1f}%)",
                batchSizeOptimo, batchSizeBase, porcentajeLibre * 100);

        return batchSizeOptimo;
    }

    private ParametrosFiltrosDTO crearFiltrosParaLote(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            int offset,
            String provincia) {

        boolean usarKeyset = offset > 0 && lastKeyPerProvince.containsKey(provincia);

        if (usarKeyset) {
            Object[] lastKey = lastKeyPerProvince.get(provincia);

            log.debug("🔑 Keyset para {}: id={}, serie={}, lugar={}",
                    provincia, lastKey[0], lastKey[1], lastKey[2]);

            return filtrosOriginales.toBuilder()
                    .limite(batchSize)
                    .offset(null)
                    .pagina(null)
                    .tamanoPagina(null)
                    .lastId((Integer) lastKey[0])
                    .lastSerieEquipo((String) lastKey[1])
                    .lastLugar((String) lastKey[2])
                    .build();

        } else {
            log.debug("📍 Primera página para {}: limite={}", provincia, batchSize);

            return filtrosOriginales.toBuilder()
                    .limite(batchSize)
                    .offset(null)
                    .pagina(null)
                    .tamanoPagina(null)
                    .lastId(null)
                    .lastSerieEquipo(null)
                    .lastLugar(null)
                    .build();
        }
    }

    private ParametrosFiltrosDTO crearFiltrosParaLote(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            int offset) {

        log.debug("Creando filtros SIN provincia - batchSize: {}, offset: {}", batchSize, offset);

        ParametrosFiltrosDTO filtrosLote = filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(offset)
                .pagina(null)
                .tamanoPagina(null)
                .lastId(null)
                .lastSerieEquipo(null)
                .lastLugar(null)
                .build();

        if (!filtrosLote.validarPaginacion()) {
            log.warn("Filtros de lote inválidos: {}", filtrosLote.getInfoPaginacion());
        }

        return filtrosLote;
    }

    private void escribirLoteAArchivo(List<Map<String, Object>> lote) throws IOException {
        if (archivoSalida == null) {
            throw new IllegalStateException("Archivo de salida no configurado");
        }

        for (Map<String, Object> registro : lote) {
            StringBuilder linea = new StringBuilder();
            boolean first = true;

            for (Object valor : registro.values()) {
                if (!first) linea.append(",");

                String valorStr = valor != null ? valor.toString() : "";
                if (valorStr.contains("\"") || valorStr.contains(",")) {
                    valorStr = "\"" + valorStr.replace("\"", "\"\"") + "\"";
                }
                linea.append(valorStr);
                first = false;
            }

            archivoSalida.write(linea.toString());
            archivoSalida.newLine();
        }

        archivoSalida.flush();
    }

    private void procesarColaResultados() {
        List<Map<String, Object>> loteParaProcesar = new ArrayList<>();

        for (int i = 0; i < chunkSize && !colaResultados.isEmpty(); i++) {
            Map<String, Object> registro = colaResultados.poll();
            if (registro != null) {
                loteParaProcesar.add(registro);
            }
        }

        if (!loteParaProcesar.isEmpty() && procesadorLoteGlobal != null) {
            procesadorLoteGlobal.accept(loteParaProcesar);
        }
    }

    public void finalizarProcesamiento() {
        try {
            if (!colaResultados.isEmpty()) {
                List<Map<String, Object>> restantes = new ArrayList<>(colaResultados);
                colaResultados.clear();
                if (procesadorLoteGlobal != null) {
                    procesadorLoteGlobal.accept(restantes);
                }
            }

            if (archivoSalida != null) {
                archivoSalida.close();
                archivoSalida = null;
            }

        } catch (Exception e) {
            log.error("Error finalizando procesamiento: {}", e.getMessage(), e);
        }
    }

    public void limpiarMemoria() {
        if (esMemoriaCritica()) {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            pausaInteligente();
        }
    }

    private void limpiarConexionesBD() {
        try {
            log.debug("Ejecutando limpieza de conexiones BD...");
            System.runFinalization();
            Thread.sleep(500);
            log.debug("Limpieza de conexiones BD completada");
        } catch (Exception e) {
            log.warn("Error en limpieza de conexiones BD: {}", e.getMessage());
        }
    }
}