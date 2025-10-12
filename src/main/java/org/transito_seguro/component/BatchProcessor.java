package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.service.QueryRegistryService;

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
    private QueryRegistryService queryRegistryService;;

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
        
        // ✅ OPCIÓN 1: SIEMPRE usar COUNT(*) real (más seguro)
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
        // Remover ORDER BY si existe (no necesario para contar)
        String querySinOrder = queryOriginal.replaceAll("(?i)ORDER BY[^;]*", "").trim();

        // Envolver en subquery
        return String.format("SELECT COUNT(*) as total FROM (%s) AS subquery", querySinOrder);
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
        if (lote.isEmpty())
            return;

        try {
            Map<String, Object> ultimo = lote.get(lote.size() - 1);

            // Keyset estándar (queries normales con id)
            if (ultimo.containsKey("id") && ultimo.get("id") != null) {
                lastKeyPerProvince.put(provincia, new Object[] {
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