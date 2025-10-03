package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import javax.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.batch.size:5000}")
    private int defaultBatchSize;

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

    @Value("${app.batch.thread-pool-size:6}")
    private int threadPoolSize;

    private ExecutorService parallelExecutor;

    // Cola con límite para evitar crecimiento descontrolado
    private final BlockingQueue<Map<String, Object>> colaResultados = new LinkedBlockingQueue<>(10000);

    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();
    private final AtomicInteger totalRepositoriosProcesados = new AtomicInteger(0);
    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);
    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();
    private final AtomicLong ultimoHeartbeat = new AtomicLong(0);

    private static final long HEARTBEAT_INTERVAL_MS = 30000;
    private long tiempoInicioGlobal;

    @Autowired
    private QueryStorageRepository queryStorageRepository;

    // Cache para queries consolidables
    private final Map<String, Boolean> cacheQueryConsolidable = new ConcurrentHashMap<>();

    @Autowired
    public void init() {
        this.parallelExecutor = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @PreDestroy
    public void shutdown() {
        log.info("Cerrando BatchProcessor...");
        if (parallelExecutor != null && !parallelExecutor.isShutdown()) {
            parallelExecutor.shutdown();
            try {
                if (!parallelExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    parallelExecutor.shutdownNow();
                    if (!parallelExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                        log.error("ExecutorService no se pudo cerrar");
                    }
                }
            } catch (InterruptedException e) {
                parallelExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        cacheQueryConsolidable.clear();
        log.info("BatchProcessor cerrado");
    }

    @AllArgsConstructor
    private static class EstimacionDataset {
        final int totalEstimado;
        final double promedioPorProvincia;
        final int maximoPorProvincia;
    }

    @AllArgsConstructor
    private static class ContextoProcesamiento {
        final Consumer<List<Map<String, Object>>> procesador;
        final BufferedWriter archivo;
        final boolean usarCola;
    }

    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesarLotes
    ) {
        tiempoInicioGlobal = System.currentTimeMillis();
        ultimoHeartbeat.set(tiempoInicioGlobal);
        totalRepositoriosProcesados.set(0);
        totalRegistrosGlobales.set(0);
        contadoresPorProvincia.clear();

        if (log.isInfoEnabled()) {
            log.info("═══════════════════════════════════════════════════════════");
            log.info("INICIO DEL PROCESAMIENTO");
            log.info("   Provincias: {}", repositories.size());
            log.info("   Query: {}", nombreQuery);
            log.info("   Memoria: {:.1f}%", obtenerPorcentajeMemoriaUsada());
            log.info("═══════════════════════════════════════════════════════════");
        }

        EstimacionDataset estimacion = estimarDataset(repositories, filtros, nombreQuery);

        if (log.isInfoEnabled()) {
            log.info("ESTIMACIÓN:");
            log.info("   Total: {} registros", estimacion.totalEstimado);
            log.info("   Promedio: {} reg/prov", (int) estimacion.promedioPorProvincia);
            log.info("   Máximo: {} registros", estimacion.maximoPorProvincia);
        }

        EstrategiaProcessing estrategia = decidirEstrategia(estimacion);
        log.info("ESTRATEGIA: {}", estrategia);

        ContextoProcesamiento contexto = new ContextoProcesamiento(procesarLotes, null, false);

        try {
            switch (estrategia) {
                case PARALELO:
                    procesarParalelo(repositories, filtros, nombreQuery, contexto);
                    break;
                case HIBRIDO:
                    procesarHibrido(repositories, filtros, nombreQuery, contexto, estimacion);
                    break;
                case SECUENCIAL:
                    procesarSecuencial(repositories, filtros, nombreQuery, contexto);
                    break;
            }
        } finally {
            imprimirResumenFinal();
        }
    }

    private void imprimirResumenFinal() {
        long duracionTotal = System.currentTimeMillis() - tiempoInicioGlobal;
        int total = totalRegistrosGlobales.get();

        if (log.isInfoEnabled()) {
            log.info("═══════════════════════════════════════════════════════════");
            log.info("PROCESAMIENTO COMPLETADO");
            log.info("   Duración: {}s", duracionTotal / 1000);
            log.info("   Total: {} registros", total);
            log.info("   Velocidad: {} reg/s", total * 1000 / Math.max(duracionTotal, 1));
            log.info("   Memoria: {:.1f}%", obtenerPorcentajeMemoriaUsada());
            log.info("═══════════════════════════════════════════════════════════");

            if (!contadoresPorProvincia.isEmpty()) {
                log.info("POR PROVINCIA:");
                contadoresPorProvincia.entrySet().stream()
                        .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                        .limit(10)
                        .forEach(e -> log.info("   {} -> {} registros", e.getKey(), e.getValue()));
            }
        }
    }

    private EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        // Usar COUNT(*) si la query lo soporta, sino muestra
        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(50)
                .offset(0)
                .build();

        List<Integer> muestras = repositories.parallelStream()
                .map(repo -> {
                    try {
                        List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);
                        int size = muestra.size();

                        // Extrapolación más precisa
                        if (size >= 50) {
                            // Probablemente hay más, usar múltiplo conservador
                            return size * 100; // 5000 registros estimados
                        } else if (size > 0) {
                            // Multiplicar por factor según tamaño
                            return size * 50;
                        }
                        return 0;
                    } catch (Exception e) {
                        log.warn("Error estimando {}: {}", repo.getProvincia(), e.getMessage());
                        return 1000;
                    }
                })
                .collect(Collectors.toList());

        int totalEstimado = muestras.stream().mapToInt(Integer::intValue).sum();
        double promedioPorProvincia = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximoPorProvincia = muestras.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedioPorProvincia, maximoPorProvincia);
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

    private void procesarParalelo(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        log.info("Ejecutando PARALELO");
        long startTime = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = repositories.stream()
                .map(repo -> CompletableFuture.runAsync(() ->
                                ejecutarProvinciaCompleta(repo, filtros, nombreQuery, contexto),
                        parallelExecutor
                ))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("PARALELO completado en {}ms", System.currentTimeMillis() - startTime);
    }

    private void procesarHibrido(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            EstimacionDataset estimacion) {

        log.info("Ejecutando HÍBRIDO con {} provincias paralelas", maxParallelProvinces);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
            List<InfraccionesRepositoryImpl> grupo = repositories.subList(i, endIndex);

            log.info("Grupo {}-{} de {}", i + 1, endIndex, repositories.size());

            List<CompletableFuture<Void>> futures = grupo.stream()
                    .map(repo -> CompletableFuture.runAsync(() ->
                                    ejecutarProvinciaCompleta(repo, filtros, nombreQuery, contexto),
                            parallelExecutor
                    ))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Pausa suave entre grupos si hay más
            if (endIndex < repositories.size() && esMemoriaAlta()) {
                pausaBreveSiNecesario();
            }
        }

        log.info("HÍBRIDO completado en {}ms", System.currentTimeMillis() - startTime);
    }

    private void procesarSecuencial(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        int batchSize = calcularTamanoLoteOptimo(filtros);
        int totalRepositorios = repositories.size();

        for (int i = 0; i < totalRepositorios; i++) {
            InfraccionesRepositoryImpl repo = repositories.get(i);
            String provincia = repo.getProvincia();

            if (log.isInfoEnabled()) {
                log.info("Provincia [{}/{}]: {} - Memoria: {:.1f}%",
                        i + 1, totalRepositorios, provincia, obtenerPorcentajeMemoriaUsada());
            }

            logHeartbeatSiCorresponde(totalRepositorios);

            procesarProvinciaSecuencial(repo, filtros, nombreQuery, contexto, batchSize, provincia);

            // Limpieza entre provincias solo si es necesario
            if (i < totalRepositorios - 1 && esMemoriaAlta()) {
                pausaBreveSiNecesario();
            }
        }
    }

    private void procesarProvinciaSecuencial(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            int batchSize,
            String provincia) {

        lastKeyPerProvince.remove(provincia);
        int procesadosEnProvincia = 0;
        int offset = 0;
        boolean hayMasDatos = true;

        while (hayMasDatos) {
            try {
                // Ajustar batch size si memoria crítica
                if (esMemoriaCritica()) {
                    batchSize = Math.max(500, batchSize / 2);
                    log.warn("Memoria crítica - batch reducido a {}", batchSize);
                }

                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset, provincia);
                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    hayMasDatos = false;
                    break;
                }

                // Guardar lastKey si no está vacío
                guardarLastKey(lote, provincia);

                // Procesar
                agregarProvincia(lote, provincia);
                procesarLote(lote, contexto);

                procesadosEnProvincia += lote.size();
                offset += batchSize;

                if (lote.size() < batchSize) {
                    hayMasDatos = false;
                }

                lote.clear();

            } catch (OutOfMemoryError oom) {
                log.error("OOM en {}, offset {}", provincia, offset);
                pausaBreveSiNecesario();
                break;
            } catch (Exception e) {
                log.error("Error en {}, offset {}: {}", provincia, offset, e.getMessage());
                break;
            }
        }

        contadoresPorProvincia.put(provincia, procesadosEnProvincia);
        totalRegistrosGlobales.addAndGet(procesadosEnProvincia);
        totalRepositoriosProcesados.incrementAndGet();
        lastKeyPerProvince.remove(provincia);
    }

    private void ejecutarProvinciaCompleta(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        String provincia = repo.getProvincia();
        boolean queryTieneGroupBy = esQueryConsolidable(nombreQuery);

        if (queryTieneGroupBy) {
            ejecutarQueryConGroupBy(repo, filtros, nombreQuery, provincia, contexto);
        } else {
            ejecutarQueryConPaginacion(repo, filtros, nombreQuery, provincia, contexto);
        }
    }

    private boolean esQueryConsolidable(String nombreQuery) {
        return cacheQueryConsolidable.computeIfAbsent(nombreQuery, key -> {
            try {
                Optional<QueryStorage> qs = queryStorageRepository.findByCodigo(key);
                if (qs.isPresent()) {
                    boolean consolidable = qs.get().getEsConsolidable();
                    log.debug("Query {} consolidable: {}", key, consolidable);
                    return consolidable;
                }
            } catch (Exception e) {
                log.warn("Error verificando consolidable {}: {}", key, e.getMessage());
            }
            return false;
        });
    }

    private void ejecutarQueryConGroupBy(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        try {
            ParametrosFiltrosDTO filtrosSinLimite = filtros != null ?
                    filtros.toBuilder().limite(null).offset(null).build() : null;

            List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(nombreQuery, filtrosSinLimite);

            if (resultado != null && !resultado.isEmpty()) {
                log.debug("Query GROUP BY retornó {} grupos para {}", resultado.size(), provincia);
                agregarProvincia(resultado, provincia);
                procesarLote(resultado, contexto);

                int cantidad = resultado.size();
                contadoresPorProvincia.put(provincia, cantidad);
                totalRegistrosGlobales.addAndGet(cantidad);
                resultado.clear();
            }
        } catch (Exception e) {
            log.error("Error GROUP BY {} en {}: {}", nombreQuery, provincia, e.getMessage());
        }
    }

    private void ejecutarQueryConPaginacion(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        try {
            int offset = 0;
            int maxLote = 1000;
            int procesados = 0;

            lastKeyPerProvince.remove(provincia);

            while (true) {
                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, maxLote, offset, provincia);
                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    break;
                }

                guardarLastKey(lote, provincia);
                agregarProvincia(lote, provincia);
                procesarLote(lote, contexto);

                procesados += lote.size();
                offset += maxLote;

                if (lote.size() < maxLote) {
                    break;
                }

                lote.clear();
            }

            contadoresPorProvincia.put(provincia, procesados);
            totalRegistrosGlobales.addAndGet(procesados);
            lastKeyPerProvince.remove(provincia);

        } catch (Exception e) {
            log.error("Error paginación {} en {}: {}", nombreQuery, provincia, e.getMessage());
            lastKeyPerProvince.remove(provincia);
        }
    }

    private void guardarLastKey(List<Map<String, Object>> lote, String provincia) {
        if (lote.isEmpty()) return;

        try {
            Map<String, Object> ultimo = lote.get(lote.size() - 1);

            // Validar que existan los campos antes de guardar
            if (ultimo.containsKey("id") &&
                    ultimo.containsKey("serie_equipo") &&
                    ultimo.containsKey("lugar")) {

                Object[] lastKey = new Object[]{
                        ultimo.get("id"),
                        ultimo.get("serie_equipo"),
                        ultimo.get("lugar")
                };
                lastKeyPerProvince.put(provincia, lastKey);
            } else {
                log.debug("Campos para keyset no disponibles en {}", provincia);
            }
        } catch (Exception e) {
            log.warn("Error guardando lastKey para {}: {}", provincia, e.getMessage());
        }
    }

    private void agregarProvincia(List<Map<String, Object>> lote, String provincia) {
        for (Map<String, Object> registro : lote) {
            registro.put("provincia", provincia);
        }
    }

    private void procesarLote(List<Map<String, Object>> lote, ContextoProcesamiento contexto) {
        if (lote == null || lote.isEmpty()) return;

        try {
            if (contexto.procesador != null) {
                procesarEnChunks(lote, contexto.procesador);
            } else if (contexto.archivo != null) {
                escribirLoteAArchivo(lote, contexto.archivo);
            } else if (contexto.usarCola) {
                colaResultados.addAll(lote);
            }
        } catch (Exception e) {
            log.error("Error procesando lote: {}", e.getMessage(), e);
            throw new RuntimeException("Fallo en procesamiento", e);
        }
    }

    private void procesarEnChunks(List<Map<String, Object>> lote, Consumer<List<Map<String, Object>>> procesador) {
        int currentChunkSize = Math.min(chunkSize, lote.size());

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            try {
                procesador.accept(chunk);
            } catch (Exception e) {
                log.error("Error procesando chunk: {}", e.getMessage());
                throw e;
            }
        }
    }

    private void escribirLoteAArchivo(List<Map<String, Object>> lote, BufferedWriter archivo) throws IOException {
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

            archivo.write(linea.toString());
            archivo.newLine();
        }
        archivo.flush();
    }

    private void logHeartbeatSiCorresponde(int totalRepositorios) {
        long ahora = System.currentTimeMillis();
        long ultimo = ultimoHeartbeat.get();

        if (ahora - ultimo > HEARTBEAT_INTERVAL_MS && ultimoHeartbeat.compareAndSet(ultimo, ahora)) {
            long transcurrido = ahora - tiempoInicioGlobal;
            log.info("HEARTBEAT - {}min {}s | {} registros | Memoria: {:.1f}% | Provincias: {}/{}",
                    transcurrido / 60000,
                    (transcurrido / 1000) % 60,
                    totalRegistrosGlobales.get(),
                    obtenerPorcentajeMemoriaUsada(),
                    totalRepositoriosProcesados.get(),
                    totalRepositorios);
        }
    }

    private double obtenerPorcentajeMemoriaUsada() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        long memoriaMax = runtime.maxMemory();
        return (double) memoriaUsada / memoriaMax * 100;
    }

    private boolean esMemoriaCritica() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        long memoriaMax = runtime.maxMemory();
        return ((double) memoriaUsada / memoriaMax) > memoryCriticalThreshold;
    }

    private boolean esMemoriaAlta() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        long memoriaMax = runtime.maxMemory();
        return ((double) memoriaUsada / memoriaMax) > 0.70;
    }

    private void pausaBreveSiNecesario() {
        if (esMemoriaAlta()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
        int batchSizeBase = filtros.getLimiteEfectivo();
        if (batchSizeBase < 1000) {
            batchSizeBase = defaultBatchSize;
        }

        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());
        long memoriaMax = runtime.maxMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaMax;

        int batchSizeOptimo;

        if (porcentajeLibre < 0.20) {
            batchSizeOptimo = Math.max(1000, batchSizeBase / 4);
        } else if (porcentajeLibre < 0.30) {
            batchSizeOptimo = Math.max(2000, batchSizeBase / 2);
        } else {
            batchSizeOptimo = Math.min(batchSizeBase, 10000);
        }

        log.debug("Batch size: {} (base: {}, memoria libre: {:.1f}%)",
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
}