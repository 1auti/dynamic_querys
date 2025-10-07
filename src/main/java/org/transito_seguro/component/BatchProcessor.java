package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Autowired
    private QueryStorageRepository queryStorageRepository;

    private ExecutorService parallelExecutor;
    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();
    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);
    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();
    private final AtomicLong ultimoHeartbeat = new AtomicLong(0);
    private final Map<String, Boolean> cacheQueryConsolidable = new ConcurrentHashMap<>();

    private static final long HEARTBEAT_INTERVAL_MS = 30000;
    private static final int SAMPLE_SIZE = 50;
    private static final int ESTIMATION_MULTIPLIER = 100;
    private long tiempoInicioGlobal;

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
                }
            } catch (InterruptedException e) {
                parallelExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        cacheQueryConsolidable.clear();
    }

    private static class EstimacionDataset {
        final int totalEstimado;
        final double promedioPorProvincia;
        final int maximoPorProvincia;

        EstimacionDataset(int total, double promedio, int maximo) {
            this.totalEstimado = total;
            this.promedioPorProvincia = promedio;
            this.maximoPorProvincia = maximo;
        }
    }

    private static class ContextoProcesamiento {
        final Consumer<List<Map<String, Object>>> procesador;
        final BufferedWriter archivo;
        private final ConcurrentLinkedQueue<Map<String, Object>> resultadosParciales = new ConcurrentLinkedQueue<>();
        private final ObjectMapper mapper = new ObjectMapper();

        ContextoProcesamiento(Consumer<List<Map<String, Object>>> procesador, BufferedWriter archivo) {
            this.procesador = procesador;
            this.archivo = archivo;
        }

        void agregarResultados(List<Map<String, Object>> resultados) {
            resultadosParciales.addAll(resultados);
        }

        synchronized void procesarTodosResultados() {
            if (resultadosParciales.isEmpty()) return;

            List<Map<String, Object>> todosResultados = new ArrayList<>(resultadosParciales);
            resultadosParciales.clear();

            if (procesador != null) {
                procesador.accept(todosResultados);
            } else if (archivo != null) {
                escribirArchivo(todosResultados);
            }
        }

        private void escribirArchivo(List<Map<String, Object>> resultados) {
            try {
                for (Map<String, Object> resultado : resultados) {
                    archivo.write(mapper.writeValueAsString(resultado));
                    archivo.newLine();
                }
                archivo.flush();
            } catch (IOException e) {
                throw new RuntimeException("Error escribiendo archivo", e);
            }
        }
    }

    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesarLotes) {

        inicializarProcesamiento();
        logInicio(repositories.size(), nombreQuery);

        EstimacionDataset estimacion = estimarDataset(repositories, filtros, nombreQuery);
        logEstimacion(estimacion);

        EstrategiaProcessing estrategia = decidirEstrategia(estimacion);
        log.info("Estrategia seleccionada: {}", estrategia);

        ContextoProcesamiento contexto = new ContextoProcesamiento(procesarLotes, null);

        try {
            switch (estrategia) {
                case PARALELO:
                    procesarParalelo(repositories, filtros, nombreQuery, contexto);
                    break;
                case HIBRIDO:
                    procesarHibrido(repositories, filtros, nombreQuery, contexto);
                    break;
                case SECUENCIAL:
                    procesarSecuencial(repositories, filtros, nombreQuery, contexto);
                    break;
            }
        } finally {
            imprimirResumenFinal();
        }
    }

    private void inicializarProcesamiento() {
        tiempoInicioGlobal = System.currentTimeMillis();
        ultimoHeartbeat.set(tiempoInicioGlobal);
        totalRegistrosGlobales.set(0);
        contadoresPorProvincia.clear();
    }

    private void logInicio(int numProvincias, String nombreQuery) {
        if (!log.isInfoEnabled()) return;

        log.info("═══════════════════════════════════════════════════════════");
        log.info("Inicio procesamiento - Provincias: {} | Query: {} | Memoria: {:.1f}%",
                numProvincias, nombreQuery, obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");
    }

    private void logEstimacion(EstimacionDataset estimacion) {
        if (!log.isInfoEnabled()) return;

        log.info("Estimación - Total: {} | Promedio: {} reg/prov | Máximo: {}",
                estimacion.totalEstimado,
                (int) estimacion.promedioPorProvincia,
                estimacion.maximoPorProvincia);
    }

    private void imprimirResumenFinal() {
        if (!log.isInfoEnabled()) return;

        long duracionTotal = System.currentTimeMillis() - tiempoInicioGlobal;
        int total = totalRegistrosGlobales.get();

        log.info("═══════════════════════════════════════════════════════════");
        log.info("Completado - Duración: {}s | Total: {} registros | Velocidad: {} reg/s | Memoria: {:.1f}%",
                duracionTotal / 1000,
                total,
                total * 1000 / Math.max(duracionTotal, 1),
                obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");
    }

    private EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(SAMPLE_SIZE)
                .offset(0)
                .build();

        List<Integer> muestras = repositories.parallelStream()
                .map(repo -> estimarProvincia(repo, nombreQuery, filtrosPrueba))
                .collect(Collectors.toList());

        int totalEstimado = muestras.stream().mapToInt(Integer::intValue).sum();
        double promedio = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximo = muestras.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedio, maximo);
    }

    private int estimarProvincia(InfraccionesRepositoryImpl repo, String nombreQuery, ParametrosFiltrosDTO filtros) {
        try {
            List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtros);
            int size = muestra.size();
            return size >= SAMPLE_SIZE ? size * ESTIMATION_MULTIPLIER : size * (ESTIMATION_MULTIPLIER / 2);
        } catch (Exception e) {
            log.warn("Error estimando {}: {}", repo.getProvincia(), e.getMessage());
            return 1000;
        }
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

        log.info("Ejecutando modo PARALELO");

        List<CompletableFuture<Void>> futures = repositories.stream()
                .map(repo -> CompletableFuture.runAsync(
                        () -> ejecutarProvincia(repo, filtros, nombreQuery, contexto),
                        parallelExecutor
                ))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        contexto.procesarTodosResultados();
    }

    private void procesarHibrido(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        log.info("Ejecutando modo HÍBRIDO");

        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
            List<InfraccionesRepositoryImpl> grupo = repositories.subList(i, endIndex);

            List<CompletableFuture<Void>> futures = grupo.stream()
                    .map(repo -> CompletableFuture.runAsync(
                            () -> ejecutarProvincia(repo, filtros, nombreQuery, contexto),
                            parallelExecutor
                    ))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            contexto.procesarTodosResultados();

            if (endIndex < repositories.size() && esMemoriaAlta()) {
                pausarSiNecesario();
            }
        }
    }

    private void procesarSecuencial(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        int batchSize = calcularTamanoLoteOptimo(filtros);

        for (int i = 0; i < repositories.size(); i++) {
            InfraccionesRepositoryImpl repo = repositories.get(i);

            logHeartbeat(repositories.size());
            procesarProvinciaSecuencial(repo, filtros, nombreQuery, contexto, batchSize);
            contexto.procesarTodosResultados();

            if (i < repositories.size() - 1 && esMemoriaAlta()) {
                pausarSiNecesario();
            }
        }
    }

    private void procesarProvinciaSecuencial(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            int batchSize) {

        String provincia = repo.getProvincia();
        lastKeyPerProvince.remove(provincia);

        int procesados = 0;
        int offset = 0;
        boolean continuar = true;

        while (continuar) {
            try {
                if (esMemoriaCritica()) {
                    batchSize = Math.max(500, batchSize / 2);
                }

                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset, provincia);
                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    break;
                }

                guardarLastKey(lote, provincia);

                List<Map<String, Object>> loteInmutable = crearCopiasInmutables(lote, provincia);
                contexto.agregarResultados(loteInmutable);

                procesados += lote.size();
                offset += batchSize;
                continuar = lote.size() >= batchSize;

            } catch (OutOfMemoryError oom) {
                log.error("OOM en {}", provincia);
                pausarSiNecesario();
                break;
            } catch (Exception e) {
                log.error("Error en {}: {}", provincia, e.getMessage());
                break;
            }
        }

        actualizarContadores(provincia, procesados);
        lastKeyPerProvince.remove(provincia);
    }

    private void ejecutarProvincia(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto) {

        String provincia = repo.getProvincia();

        // Verificar si debe forzar paginación
        boolean debeUsarPaginacion = Boolean.TRUE.equals(filtros.getForzarPaginacion());

        if (!debeUsarPaginacion && esQueryConsolidable(nombreQuery)) {
            ejecutarQueryConsolidable(repo, filtros, nombreQuery, provincia, contexto);
        } else {
            ejecutarQueryPaginada(repo, filtros, nombreQuery, provincia, contexto);
        }
    }

    private boolean esQueryConsolidable(String nombreQuery) {
        return cacheQueryConsolidable.computeIfAbsent(nombreQuery, key -> {
            try {
                Optional<QueryStorage> qs = queryStorageRepository.findByCodigo(key);
                return qs.map(QueryStorage::getEsConsolidable).orElse(false);
            } catch (Exception e) {
                return false;
            }
        });
    }

    private void ejecutarQueryConsolidable(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        try {
            ParametrosFiltrosDTO filtrosSinLimite = filtros.toBuilder()
                    .limite(null)
                    .offset(null)
                    .build();

            List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(nombreQuery, filtrosSinLimite);

            if (resultado != null && !resultado.isEmpty()) {
                List<Map<String, Object>> resultadoInmutable = crearCopiasInmutables(resultado, provincia);
                contexto.agregarResultados(resultadoInmutable);
                actualizarContadores(provincia, resultado.size());
            }
        } catch (Exception e) {
            log.error("Error en query consolidable {} para {}: {}", nombreQuery, provincia, e.getMessage());
        }
    }

    private void ejecutarQueryPaginada(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        String provinciaRepo = repo.getProvincia();
        lastKeyPerProvince.remove(provinciaRepo);

        int procesados = 0;
        int offset = 0;
        int maxLote = 1000;

        while (true) {
            try {
                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, maxLote, offset, provinciaRepo);
                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    break;
                }

                guardarLastKey(lote, provinciaRepo);

                List<Map<String, Object>> loteInmutable = crearCopiasInmutables(lote, provinciaRepo);
                contexto.agregarResultados(loteInmutable);

                procesados += lote.size();
                offset += maxLote;

                if (lote.size() < maxLote) {
                    break;
                }

            } catch (Exception e) {
                log.error("Error en paginación {} para {}: {}", nombreQuery, provinciaRepo, e.getMessage());
                break;
            }
        }

        actualizarContadores(provinciaRepo, procesados);
        lastKeyPerProvince.remove(provinciaRepo);
    }

    private List<Map<String, Object>> crearCopiasInmutables(List<Map<String, Object>> registros, String provincia) {
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

    private void actualizarContadores(String provincia, int cantidad) {
        contadoresPorProvincia.put(provincia, cantidad);
        totalRegistrosGlobales.addAndGet(cantidad);
    }

    private void guardarLastKey(List<Map<String, Object>> lote, String provincia) {
        if (lote.isEmpty()) return;

        try {
            Map<String, Object> ultimo = lote.get(lote.size() - 1);

            // Keyset estándar (queries normales con id)
            if (ultimo.containsKey("id") && ultimo.get("id") != null) {
                lastKeyPerProvince.put(provincia, new Object[]{
                        ultimo.get("id"),
                        ultimo.get("serie_equipo"),
                        ultimo.get("lugar")
                });
                log.debug("Keyset estándar guardado: id={}", ultimo.get("id"));
            }
            // Keyset consolidación (primeros campos disponibles)
            else {
                // Tomar los primeros 3 campos del registro
                List<Object> keyValues = new ArrayList<>();
                int count = 0;

                for (Map.Entry<String, Object> entry : ultimo.entrySet()) {
                    if (entry.getValue() != null && count < 3) {
                        keyValues.add(entry.getValue());
                        count++;
                    }
                }

                if (!keyValues.isEmpty()) {
                    lastKeyPerProvince.put(provincia, keyValues.toArray());
                    log.debug("Keyset consolidación guardado: {} campos = {}",
                            keyValues.size(), keyValues);
                }
            }
        } catch (Exception e) {
            log.debug("Error guardando lastKey para {}: {}", provincia, e.getMessage());
        }
    }

    private void logHeartbeat(int totalRepositorios) {
        long ahora = System.currentTimeMillis();
        long ultimo = ultimoHeartbeat.get();

        if (ahora - ultimo > HEARTBEAT_INTERVAL_MS && ultimoHeartbeat.compareAndSet(ultimo, ahora)) {
            long transcurrido = ahora - tiempoInicioGlobal;
            log.info("Heartbeat - {}s | {} registros | Memoria: {:.1f}%",
                    transcurrido / 1000,
                    totalRegistrosGlobales.get(),
                    obtenerPorcentajeMemoriaUsada());
        }
    }

    private double obtenerPorcentajeMemoriaUsada() {
        Runtime runtime = Runtime.getRuntime();
        return (double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory() * 100;
    }

    private boolean esMemoriaCritica() {
        Runtime runtime = Runtime.getRuntime();
        return ((double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory()) > memoryCriticalThreshold;
    }

    private boolean esMemoriaAlta() {
        Runtime runtime = Runtime.getRuntime();
        return ((double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory()) > 0.70;
    }

    private void pausarSiNecesario() {
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
        double porcentajeLibre = (double) memoriaLibre / runtime.maxMemory();

        if (porcentajeLibre < 0.20) {
            return Math.max(1000, batchSizeBase / 4);
        } else if (porcentajeLibre < 0.30) {
            return Math.max(2000, batchSizeBase / 2);
        }
        return Math.min(batchSizeBase, 10000);
    }

    private ParametrosFiltrosDTO crearFiltrosParaLote(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            int offset,
            String provincia) {

        boolean usarKeyset = offset > 0 && lastKeyPerProvince.containsKey(provincia);

        if (usarKeyset) {
            Object[] lastKey = lastKeyPerProvince.get(provincia);

            // Detectar tipo de keyset
            if (lastKey.length >= 1 && lastKey[0] instanceof Integer &&
                    lastKey.length == 3) {
                // Keyset estándar (id, serie_equipo, lugar)
                return filtrosOriginales.toBuilder()
                        .limite(batchSize)
                        .offset(null)
                        .lastId((Integer) lastKey[0])
                        .lastSerieEquipo((String) lastKey[1])
                        .lastLugar((String) lastKey[2])
                        .build();
            } else {
                // Keyset consolidación genérico
                Map<String, Object> keysetMap = new HashMap<>();
                for (int i = 0; i < Math.min(lastKey.length, 3); i++) {
                    if (lastKey[i] != null) {
                        keysetMap.put("campo_" + i, lastKey[i]);
                    }
                }

                return filtrosOriginales.toBuilder()
                        .limite(batchSize)
                        .offset(null)
                        .lastKeysetConsolidacion(keysetMap)
                        .build();
            }
        }

        // Primera iteración: sin keyset
        return filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(null)
                .lastId(null)
                .lastSerieEquipo(null)
                .lastLugar(null)
                .lastKeysetConsolidacion(null)
                .build();
    }
}