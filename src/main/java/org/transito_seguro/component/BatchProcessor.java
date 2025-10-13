package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.service.QueryRegistryService;
import org.transito_seguro.utils.LogFileWriter;

import javax.annotation.PreDestroy;
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
    private QueryRegistryService queryRegistryService;;

    @Autowired
    private LogFileWriter logFileWriter;

    private ExecutorService parallelExecutor;
    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();
    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);
    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();
    private final AtomicLong ultimoHeartbeat = new AtomicLong(0);
    private final Map<String, Boolean> cacheQueryConsolidable = new ConcurrentHashMap<>();

    private static final long HEARTBEAT_INTERVAL_MS = 30000;

    private long tiempoInicioGlobal;

    @Autowired
    public void init() {
        this.parallelExecutor = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
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

        QueryStorage queryStorage = queryRegistryService.buscarQuery(nombreQuery)
                .orElse(null);

        try {
            switch (estrategia) {
                case PARALELO:
                    procesarParalelo(repositories, filtros, nombreQuery, contexto,queryStorage);
                    break;
                case HIBRIDO:
                    procesarHibrido(repositories, filtros, nombreQuery, contexto,queryStorage);
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
        if (!log.isInfoEnabled())
            return;

        log.info("═══════════════════════════════════════════════════════════");
        log.info("Inicio procesamiento - Provincias: {} | Query: {} | Memoria: {:.1f}%",
                numProvincias, nombreQuery, obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");
    }

    private void logEstimacion(EstimacionDataset estimacion) {
        if (!log.isInfoEnabled())
            return;

        log.info("Estimación - Total: {} | Promedio: {} reg/prov | Máximo: {}",
                estimacion.getTotalEstimado(),
                (int) estimacion.getPromedioPorProvincia(),
                estimacion.getMaximoPorProvincia());
    }

    private void imprimirResumenFinal() {
        if (!log.isInfoEnabled())
            return;

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

    /**
     * Estima el tamaño del dataset usando COUNT(*) en lugar de cargar datos.
     * 
     * @param repositories Lista de repositorios (uno por provincia)
     * @param filtros      Filtros aplicados (fechas, tipos, etc.)
     * @param nombreQuery  Código de la query a ejecutar
     * @return Estimación con total real, promedio y máximo por provincia
     */
    private EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        // Ejecutar COUNT(*) en paralelo para todas las provincias
        List<Integer> conteos = repositories.parallelStream()
                .map(repo -> obtenerConteoReal(repo, nombreQuery, filtros))
                .collect(Collectors.toList());

        // Calcular estadísticas
        int totalEstimado = conteos.stream().mapToInt(Integer::intValue).sum();
        double promedio = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximo = conteos.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedio, maximo);
    }

/*
 * Obtiene el conteo real de registros usando COUNT(*) O estimación del análisis.
 * Si el QueryAnalyzer ya proporcionó una estimación confiable, la usa.
 * 
 * @param repo        Repositorio de la provincia
 * @param nombreQuery Código de la query
 * @param filtros     Filtros aplicados
 * @return Número exacto o estimado de registros
 */
private int obtenerConteoReal(
        InfraccionesRepositoryImpl repo,
        String nombreQuery,
        ParametrosFiltrosDTO filtros) {
    try {
        Optional<QueryStorage> queryOpt = queryRegistryService.buscarQuery(nombreQuery);
        if (!queryOpt.isPresent()) {
            log.warn("Query no encontrada: {} para provincia {}", nombreQuery, repo.getProvincia());
            return 0;
        }

        QueryStorage queryStorage = queryOpt.get();
        
        /**
         * ES mejor usar el count real porque es maala idea a la larga no estimar la cantidad de registros 
         * que podes tener :) 
         */
     
        String queryOriginal = queryStorage.getSqlQuery();
        String queryConteo = construirQueryConteo(queryOriginal);
        Integer conteoReal = repo.ejecutarQueryConteo(queryConteo, filtros);
        
        log.info("🔍 Conteo REAL para {}: {} registros (estimación previa: {})", 
                 repo.getProvincia(), 
                 conteoReal,
                 queryStorage.getRegistrosEstimados());
                 
        return conteoReal != null ? conteoReal : 0;

    } catch (Exception e) {
        log.error("Error obteniendo conteo para {} - {}: {}",
                repo.getProvincia(), nombreQuery, e.getMessage());
        return 0;
    }
}

    /**
     * Envuelve la query original en un SELECT COUNT(*).
     * 
     * @param queryOriginal Query SQL original
     * @return Query modificada para contar registros
     */
    private String construirQueryConteo(String queryOriginal) {
        if (queryOriginal == null || queryOriginal.trim().isEmpty()) {
            throw new IllegalArgumentException("Query original no puede estar vacía");
        }

        String queryLimpia = queryOriginal.trim();

        // 1. Remover el ; final si existe
        queryLimpia = queryLimpia.replaceAll(";\\s*$", "");

        // 2. Remover ORDER BY (innecesario para contar y puede afectar performance)
        queryLimpia = queryLimpia.replaceAll("(?i)\\s+ORDER\\s+BY\\s+[^;]+$", "");

        // 3. Remover LIMIT/OFFSET si existen (afectarían el conteo)
        queryLimpia = queryLimpia.replaceAll("(?i)\\s+LIMIT\\s+\\d+", "");
        queryLimpia = queryLimpia.replaceAll("(?i)\\s+OFFSET\\s+\\d+", "");

        // 4. Envolver en subquery
        return String.format("SELECT COUNT(*) as total FROM (%s) AS subquery", queryLimpia.trim());
    }

    private EstrategiaProcessing decidirEstrategia(EstimacionDataset estimacion) {
        if (estimacion.getPromedioPorProvincia() < parallelThresholdPerProvince &&
                estimacion.getTotalEstimado() < parallelThresholdTotal) {
            return EstrategiaProcessing.PARALELO;
        }
        if (estimacion.getMaximoPorProvincia() > massiveThresholdPerProvince) {
            return EstrategiaProcessing.SECUENCIAL;
        }
        return EstrategiaProcessing.HIBRIDO;
    }

    private void procesarParalelo(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("Ejecutando modo PARALELO");

        List<CompletableFuture<Void>> futures = repositories.stream()
                .map(repo -> CompletableFuture.runAsync(
                        () -> ejecutarProvincia(repo, filtros, nombreQuery, contexto,queryStorage),
                        parallelExecutor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        contexto.procesarTodosResultados();
    }

    private void procesarHibrido(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("Ejecutando modo HÍBRIDO");

        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
            List<InfraccionesRepositoryImpl> grupo = repositories.subList(i, endIndex);

            List<CompletableFuture<Void>> futures = grupo.stream()
                    .map(repo -> CompletableFuture.runAsync(
                            () -> ejecutarProvincia(repo, filtros, nombreQuery, contexto,queryStorage),
                            parallelExecutor))
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

   /**
 * Ejecuta una provincia decidiendo la estrategia según el tipo de query.
 * 
 * @param repo Repositorio de la provincia
 * @param filtros Filtros aplicados
 * @param nombreQuery Código de la query
 * @param contexto Contexto de procesamiento
 */
private void ejecutarProvincia(
        InfraccionesRepositoryImpl repo,
        ParametrosFiltrosDTO filtros,
        String nombreQuery,
        ContextoProcesamiento contexto,
        QueryStorage queryStorage) {

    String provincia = repo.getProvincia();

    
    // Verificar si debe forzar paginación (override manual)
      boolean estrategiaSinPaginacion = queryStorage.getEstrategiaPaginacion() == EstrategiaPaginacion.SIN_PAGINACION;
    
     // Si la query no es consolidable, siempre usar paginación
    if (estrategiaSinPaginacion && esQueryConsolidable(nombreQuery)) {
        
        // Decidir estrategia según tipo de consolidación
        if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.AGREGACION) {
            // Query bien diseñada (GROUP BY con pocos registros): carga directa
            ejecutarQueryConsolidableAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            
        } else if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.CRUDO) {
            // Query mal diseñada (sin GROUP BY o muchos registros): streaming
            ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            
        } else if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.DEDUPLICACION) {
            // Query con deduplicación: carga directa si < 10K, sino streaming
            if (queryStorage.getRegistrosEstimados() != null && queryStorage.getRegistrosEstimados() < 10000) {
                ejecutarQueryConsolidableAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            } else {
                ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
            }
            
        } else {
            // Tipo desconocido, JERARQUICA o COMBINADA: usar estrategia conservadora (streaming)
            log.warn("Tipo de consolidación {} para {}, usando streaming", 
                     queryStorage.getTipoConsolidacion(), nombreQuery);
            ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
        }
    } else {
        // Query normal: usar paginación estándar
        ejecutarQueryPaginada(repo, filtros, nombreQuery, provincia, contexto);
    }
}

    private boolean esQueryConsolidable(String nombreQuery) {
        return cacheQueryConsolidable.computeIfAbsent(nombreQuery, key -> {
            try {
                Optional<QueryStorage> qs = queryRegistryService.buscarQuery(key);
                return qs.map(QueryStorage::getEsConsolidable).orElse(false);
            } catch (Exception e) {
                return false;
            }
        });
    }


    /**
 * Ejecuta query consolidable AGREGACION (bien diseñada).
 * La query YA hace GROUP BY en la BD y retorna pocos registros.
 * Es seguro cargar todo en memoria de una vez.
 * 
 * Ejemplo: SELECT provincia, COUNT(*) FROM infracciones GROUP BY provincia
 * Retorna: ~24 registros
 * 
 * @param repo Repositorio de la provincia
 * @param filtros Filtros aplicados
 * @param nombreQuery Código de la query
 * @param provincia Nombre de la provincia
 * @param contexto Contexto de procesamiento
 * @param info Información de la query
 */
private void ejecutarQueryConsolidableAgregacion(
        InfraccionesRepositoryImpl repo,
        ParametrosFiltrosDTO filtros,
        String nombreQuery,
        String provincia,
        ContextoProcesamiento contexto,
        QueryStorage info) {

    try {
        log.debug("Ejecutando query consolidable AGREGACION para {}: {} registros estimados", 
                  provincia, info.getRegistrosEstimados());

        // Ejecutar sin límite ni offset (query ya está agregada)
        ParametrosFiltrosDTO filtrosSinLimite = filtros.toBuilder()
                .limite(null)
                .offset(null)
                .build();

        // Carga directa en memoria (seguro porque retorna pocos registros)
        List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(nombreQuery, filtrosSinLimite);

        if (resultado != null && !resultado.isEmpty()) {
            // Verificar que la estimación era correcta
            if (info.getRegistrosEstimados() != null && resultado.size() > info.getRegistrosEstimados() * 10) {
                log.warn(" Query {} retornó {} registros pero se estimaban {}. Considerar cambiar a CRUDO",
                         nombreQuery, resultado.size(), info.getRegistrosEstimados());
            }
            
            // Procesar resultados
            List<Map<String, Object>> resultadoInmutable = crearCopiasInmutables(resultado, provincia);
            contexto.agregarResultados(resultadoInmutable);
            actualizarContadores(provincia, resultado.size());
            
            log.info(" Query consolidable AGREGACION completada para {}: {} registros | Memoria: {:.1f}%", 
                     provincia, resultado.size(), obtenerPorcentajeMemoriaUsada());
        } else {
            log.debug("Query consolidable AGREGACION para {} no retornó resultados", provincia);
        }
        
    } catch (OutOfMemoryError oom) {
        log.error(" OOM en query consolidable AGREGACION para {}. La query debería ser CRUDO", provincia);
        throw oom;
    } catch (Exception e) {
        log.error("Error en query consolidable AGREGACION {} para {}: {}", 
                  nombreQuery, provincia, e.getMessage());
    }
}



/**
 * Ejecuta query consolidable CRUDO (mal diseñada).
 * La query retorna datos sin agregar y puede tener muchos registros.
 * Usa streaming para evitar OutOfMemoryError y consolida en memoria.
 * 
 * Ejemplo: SELECT fecha, tipo, monto FROM infracciones WHERE...
 * Retorna: Miles de registros que deben ser consolidados
 * 
 * @param repo Repositorio de la provincia
 * @param filtros Filtros aplicados
 * @param nombreQuery Código de la query
 * @param provincia Nombre de la provincia
 * @param contexto Contexto de procesamiento
 * @param info Información de la query
 */
private void ejecutarQueryConsolidableCrudo(
        InfraccionesRepositoryImpl repo,
        ParametrosFiltrosDTO filtros,
        String nombreQuery,
        String provincia,
        ContextoProcesamiento contexto,
        QueryStorage info) {

    try {
        log.info("Ejecutando query consolidable CRUDO con streaming para {}: {} registros estimados",
                 provincia, info.getRegistrosEstimados());

        // Configurar para usar streaming
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

        // Actualizar contador global
        actualizarContadores(provincia, totalProcesados.get());
        
        log.info(" Query consolidable CRUDO completada para {}: {} registros en {} chunks | Memoria: {:.1f}%", 
                 provincia, 
                 totalProcesados.get(),
                 chunksEnviados.get(),
                 obtenerPorcentajeMemoriaUsada());

    } catch (Exception e) {
        log.error(" Error en query consolidable CRUDO para {}: {}", provincia, e.getMessage(), e);
    }
}

/**
 * Procesa un chunk de registros y lo envía al contexto.
 * Libera memoria después de procesar.
 * 
 * @param buffer Buffer con registros a procesar
 * @param provincia Nombre de la provincia
 * @param contexto Contexto de procesamiento
 * @param totalProcesados Contador total de registros procesados
 * @param chunksEnviados Contador de chunks enviados
 */
private void procesarChunk(
        List<Map<String, Object>> buffer,
        String provincia,
        ContextoProcesamiento contexto,
        AtomicInteger totalProcesados,
        AtomicInteger chunksEnviados) {
    
    // SOLO procesar si el buffer NO está vacío
    if(!buffer.isEmpty()){
        
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
            log.debug("Provincia {}: {} registros procesados en {} chunks | Memoria: {:.1f}%", 
                      provincia, 
                      totalProcesados.get(),
                      chunksEnviados.get(),
                      obtenerPorcentajeMemoriaUsada());
        }
        
        // Vaciar buffer para liberar memoria
        buffer.clear();
    }
    
    // Pausa si memoria alta
    if (esMemoriaAlta()) {
        pausarSiNecesario();
    }
}

    /**
     * Ejecuta query paginada usando un contador externo simulado.
     * Para queries con GROUP BY que no tienen campo ID y queremos evitar OFFSET.
     *
     * ESTRATEGIA:
     * - Usa un contador incremental como "pseudo-ID"
     * - La query debe retornar TODOS los registros ordenados consistentemente
     * - El procesamiento se hace por chunks en memoria
     *
     * @param repo Repositorio de la provincia
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @param provincia Nombre de la provincia
     * @param contexto Contexto de procesamiento
     */
    private void ejecutarQueryPaginada(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto) {

        String provinciaRepo = repo.getProvincia();
        int procesados = 0;
        int iteracion = 0;
        final int batchSize = 1000;

        // ✅ Contador externo que simula el "lastId"
        int contadorPaginacion = 0;

        log.debug("🚀 Iniciando paginación con contador externo para {}", provinciaRepo);

        while (true) {
            try {
                // ✅ Crear filtros usando el contador como si fuera un ID
                ParametrosFiltrosDTO filtrosLote = filtros.toBuilder()
                        .limite(batchSize)
                        .offset(null)  // ❌ NO usar offset
                        .lastId(contadorPaginacion)  // ✅ Usar contador como "pseudo-ID"
                        .lastSerieEquipo(null)
                        .lastLugar(null)
                        .lastKeysetConsolidacion(null)
                        .build();

                log.debug("📄 Iteración {}: LIMIT {} con contador={}",
                        iteracion, batchSize, contadorPaginacion);

                // Ejecutar query
                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                // Verificar si hay datos
                if (lote == null || lote.isEmpty()) {
                    log.debug("✅ Fin de datos para {} en iteración {}", provinciaRepo, iteracion);
                    break;
                }

                // Procesar resultados
                List<Map<String, Object>> loteInmutable = crearCopiasInmutables(lote, provinciaRepo);
                contexto.agregarResultados(loteInmutable);

                // Actualizar contadores
                procesados += lote.size();
                contadorPaginacion += lote.size();  // ✅ Incrementar contador externo
                iteracion++;

                log.debug("📦 Lote {} procesado: {} registros (contador: {} → {}, total: {})",
                        iteracion, lote.size(), contadorPaginacion - lote.size(),
                        contadorPaginacion, procesados);

                // ✅ Condición de salida: lote incompleto indica última página
                if (lote.size() < batchSize) {
                    log.debug("✅ Último lote para {}: {} < {}", provinciaRepo, lote.size(), batchSize);
                    break;
                }

                // Control de seguridad
                if (iteracion >= 1000) {
                    log.warn("⚠️ Límite de iteraciones alcanzado para {}: {} iteraciones",
                            provinciaRepo, iteracion);
                    break;
                }

                // Pausa si memoria alta
                if (esMemoriaAlta()) {
                    pausarSiNecesario();
                }

            } catch (Exception e) {
                log.error("❌ Error en iteración {} para {}: {}", iteracion, provinciaRepo, e.getMessage(), e);
                break;
            }
        }

        log.info("✅ Paginación completada para {}: {} registros en {} iteraciones",
                provinciaRepo, procesados, iteracion);

        // Actualizar contadores globales
        actualizarContadores(provinciaRepo, procesados);
    }


    /**
     * Obtiene el último ID del lote actual
     */
    private Integer obtenerUltimoIdDelLote(List<Map<String, Object>> lote) {
        if (lote == null || lote.isEmpty()) {
            return null;
        }

        Map<String, Object> ultimoRegistro = lote.get(lote.size() - 1);

        // Buscar campo ID con diferentes nombres posibles
        String[] posiblesCamposId = {"id", "infraccion_id", "registro_id", "consecutivo"};

        for (String campo : posiblesCamposId) {
            Object valor = ultimoRegistro.get(campo);
            if (valor instanceof Integer) {
                return (Integer) valor;
            } else if (valor instanceof Long) {
                return ((Long) valor).intValue();
            } else if (valor instanceof String) {
                try {
                    return Integer.parseInt((String) valor);
                } catch (NumberFormatException e) {
                    // Continuar con siguiente campo
                }
            }
        }

        log.warn("⚠️ No se encontró campo ID válido en el último registro. Campos disponibles: {}",
                ultimoRegistro.keySet());
        return null;
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

    /**
     * MÉTODO CORREGIDO: Guarda lastKey detectando el tipo correcto
     *
     * PROBLEMA ANTERIOR:
     * - Asumía que lastKey[0] siempre era Integer
     * - Queries de consolidación NO tienen campo 'id'
     *
     * SOLUCIÓN:
     * - Guardar los valores tal como vienen
     * - NO asumir tipos en guardarLastKey
     * - Validar tipos en crearFiltrosParaLoteSiguiente
     */
    private void guardarLastKey(List<Map<String, Object>> lote, String provincia) {
        if (lote == null || lote.isEmpty()) {
            return;
        }

        try {
            Map<String, Object> ultimo = lote.get(lote.size() - 1);

            // CASO 1: Query estándar con campo 'id'
            if (ultimo.containsKey("id") && ultimo.get("id") != null) {
                Object id = ultimo.get("id");

                lastKeyPerProvince.put(provincia, new Object[] {
                        id,
                        ultimo.get("serie_equipo"),
                        ultimo.get("lugar")
                });

                log.debug("🔑 Keyset estándar guardado para {}: id={} (tipo: {})",
                        provincia, id, id.getClass().getSimpleName());
            }
            // CASO 2: Query de consolidación SIN campo 'id'
            else {
                // Tomar los primeros 3 campos disponibles (sin asumir tipos)
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

                    log.debug("🔑 Keyset consolidación guardado para {}: {} campos (tipos: {})",
                            provincia,
                            keyValues.size(),
                            keyValues.stream()
                                    .map(v -> v.getClass().getSimpleName())
                                    .collect(Collectors.joining(", ")));
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ Error guardando lastKey para {}: {}", provincia, e.getMessage());
        }
    }

   private void logHeartbeat(int totalRepositorios) {
    long ahora = System.currentTimeMillis();
    long ultimo = ultimoHeartbeat.get();
    
    if (ahora - ultimo > HEARTBEAT_INTERVAL_MS) {
        // Actualizar sin condición de carrera
        ultimoHeartbeat.set(ahora);
        log.info("Heartbeat - {}s | {} registros | Memoria: {:.1f}%",
                (ahora - tiempoInicioGlobal) / 1000,
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
        return ((double) (runtime.totalMemory() - runtime.freeMemory())
                / runtime.maxMemory()) > memoryCriticalThreshold;
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

    /**
     * MÉTODO CORREGIDO: crearFiltrosParaLote
     *
     * CAMBIOS:
     * 1. Primera iteración YA usa límite razonable (no Integer.MAX_VALUE)
     * 2. Keyset se activa desde la SEGUNDA iteración
     * 3. NUNCA usa offset (solo keyset)
     */
    private ParametrosFiltrosDTO crearFiltrosParaLote(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            int offset,
            String provincia) {

        // PRIMERA ITERACIÓN: offset=0, sin keyset aún
        if (offset == 0 || !lastKeyPerProvince.containsKey(provincia)) {
            log.debug("🔹 Primera iteración para {}: limite={}, keyset=INACTIVO", provincia, batchSize);

            return filtrosOriginales.toBuilder()
                    .limite(batchSize)  // CRÍTICO: Usar límite razonable
                    .offset(null)       // NUNCA usar offset
                    .lastId(null)
                    .lastSerieEquipo(null)
                    .lastLugar(null)
                    .lastKeysetConsolidacion(null)
                    .build();
        }

        // ITERACIONES SIGUIENTES: usar keyset
        Object[] lastKey = lastKeyPerProvince.get(provincia);

        // Detectar tipo de keyset
        if (esKeysetEstandar(lastKey)) {
            // Keyset estándar (id, serie_equipo, lugar)
            log.debug("🔑 Keyset estándar para {}: id={}", provincia, lastKey[0]);

            return filtrosOriginales.toBuilder()
                    .limite(batchSize)
                    .offset(null)  // NUNCA usar offset con keyset
                    .lastId((Integer) lastKey[0])
                    .lastSerieEquipo(lastKey.length > 1 ? (String) lastKey[1] : null)
                    .lastLugar(lastKey.length > 2 ? (String) lastKey[2] : null)
                    .build();

        } else {
            // Keyset consolidación genérico
            Map<String, Object> keysetMap = construirKeysetMap(lastKey);

            log.debug("🔑 Keyset consolidación para {}: {} campos", provincia, keysetMap.size());

            return filtrosOriginales.toBuilder()
                    .limite(batchSize)
                    .offset(null)
                    .lastKeysetConsolidacion(keysetMap)
                    .build();
        }
    }

    /**
     * Detecta si es keyset estándar (con id numérico)
     */
    private boolean esKeysetEstandar(Object[] lastKey) {
        return lastKey != null
                && lastKey.length >= 1
                && lastKey[0] instanceof Integer;
    }

    /**
     * Construye mapa de keyset para queries de consolidación
     */
    private Map<String, Object> construirKeysetMap(Object[] lastKey) {
        Map<String, Object> keysetMap = new HashMap<>();

        for (int i = 0; i < Math.min(lastKey.length, 3); i++) {
            if (lastKey[i] != null) {
                keysetMap.put("campo_" + i, lastKey[i]);
            }
        }

        return keysetMap;
    }



    /**
     * NUEVO MÉTODO: Consolidar resultados por mes/año
     *
     * Para agregar al BatchProcessor.java
     *
     * USO:
     * List<Map<String, Object>> consolidado = consolidarPorMes(
     *     resultados,
     *     Arrays.asList("provincia", "mes", "anio")
     * );
     */
    public List<Map<String, Object>> consolidarPorMes(
            List<Map<String, Object>> resultados,
            List<String> camposAgrupacion) {

        if (resultados == null || resultados.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("🔧 Consolidando {} registros por: {}", resultados.size(), camposAgrupacion);
        long inicio = System.currentTimeMillis();

        // PASO 1: Enriquecer con campos calculados (mes, año)
        List<Map<String, Object>> enriquecidos = resultados.stream()
                .map(this::agregarCamposMesAnio)
                .collect(Collectors.toList());

        // PASO 2: Agrupar por campos solicitados
        Map<String, List<Map<String, Object>>> grupos = enriquecidos.stream()
                .collect(Collectors.groupingBy(registro ->
                        generarClaveAgrupacion(registro, camposAgrupacion)
                ));

        // PASO 3: Sumar cantidades por grupo
        List<Map<String, Object>> consolidado = grupos.entrySet().stream()
                .map(entry -> consolidarGrupo(entry.getValue(), camposAgrupacion))
                .collect(Collectors.toList());

        long duracion = System.currentTimeMillis() - inicio;
        log.info("✅ Consolidación completada en {}ms: {} → {} registros",
                duracion, resultados.size(), consolidado.size());

        return consolidado;
    }

    /**
     * Agrega campos mes y año desde fecha_constatacion
     */
    private Map<String, Object> agregarCamposMesAnio(Map<String, Object> registro) {
        Map<String, Object> enriquecido = new HashMap<>(registro);

        Object fecha = registro.get("fecha_constatacion");

        if (fecha instanceof java.sql.Date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime((java.sql.Date) fecha);
            enriquecido.put("mes", cal.get(Calendar.MONTH) + 1);
            enriquecido.put("anio", cal.get(Calendar.YEAR));

        } else if (fecha instanceof java.time.LocalDate) {
            java.time.LocalDate localDate = (java.time.LocalDate) fecha;
            enriquecido.put("mes", localDate.getMonthValue());
            enriquecido.put("anio", localDate.getYear());

        } else if (fecha instanceof java.util.Date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime((java.util.Date) fecha);
            enriquecido.put("mes", cal.get(Calendar.MONTH) + 1);
            enriquecido.put("anio", cal.get(Calendar.YEAR));
        }

        return enriquecido;
    }

    /**
     * Genera clave única para agrupar registros
     */
    private String generarClaveAgrupacion(
            Map<String, Object> registro,
            List<String> camposAgrupacion) {

        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.get(campo)))
                .collect(Collectors.joining("|"));
    }

    /**
     * Consolida un grupo sumando cantidades
     */
    private Map<String, Object> consolidarGrupo(
            List<Map<String, Object>> grupo,
            List<String> camposAgrupacion) {

        Map<String, Object> consolidado = new HashMap<>();

        // Tomar valores de agrupación del primer registro
        Map<String, Object> primero = grupo.get(0);
        for (String campo : camposAgrupacion) {
            consolidado.put(campo, primero.get(campo));
        }

        // Sumar cantidad
        int cantidadTotal = grupo.stream()
                .mapToInt(r -> {
                    Object cant = r.get("cantidad");
                    if (cant instanceof Number) {
                        return ((Number) cant).intValue();
                    }
                    return 1; // Si no hay cantidad, contar como 1
                })
                .sum();

        consolidado.put("cantidad", cantidadTotal);

        return consolidado;
    }

}