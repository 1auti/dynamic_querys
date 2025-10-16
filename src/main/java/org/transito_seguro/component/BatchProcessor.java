//package org.transito_seguro.component;
//
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Lazy;
//import org.springframework.stereotype.Component;
//import org.transito_seguro.dto.ParametrosFiltrosDTO;
//import org.transito_seguro.enums.EstrategiaPaginacion;
//import org.transito_seguro.enums.EstrategiaProcessing;
//import org.transito_seguro.enums.TipoConsolidacion;
//import org.transito_seguro.model.EstimacionDataset;
//import org.transito_seguro.model.query.QueryStorage;
//import org.transito_seguro.model.ContextoProcesamiento;
//import org.transito_seguro.repository.QueryStorageRepository;
//import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
//import org.transito_seguro.service.QueryRegistryService;
//import org.transito_seguro.utils.LogFileWriter;
//
//import javax.annotation.PreDestroy;
//import java.math.BigDecimal;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;
//
//@Slf4j
//@Component
//public class BatchProcessor {
//
//    @Value("${app.batch.size:5000}")
//    private int defaultBatchSize;
//
//    @Value("${app.batch.memory-critical-threshold:0.85}")
//    private double memoryCriticalThreshold;
//
//    @Value("${app.batch.parallel-threshold-per-province:50000}")
//    private int parallelThresholdPerProvince;
//
//    @Value("${app.batch.parallel-threshold-total:300000}")
//    private int parallelThresholdTotal;
//
//    @Value("${app.batch.massive-threshold-per-province:200000}")
//    private int massiveThresholdPerProvince;
//
//    @Value("${app.batch.max-parallel-provinces:6}")
//    private int maxParallelProvinces;
//
//    @Value("${app.batch.thread-pool-size:6}")
//    private int threadPoolSize;
//
//    @Value("${consolidacion.agregacion.umbral-error:10}")
//    private int umbralErrorEstimacion;
//
//    @Value("${consolidacion.agregacion.limite-validacion:10000}")
//    private int limiteValidacion;
//
//    @Value("${consolidacion.agregacion.limite-absoluto:100000}")
//    private int limiteAbsoluto;
//
//    @Autowired
//    private QueryRegistryService queryRegistryService;;
//
//    @Autowired
//    private LogFileWriter logFileWriter;
//
//    private ExecutorService parallelExecutor;
//    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();
//    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);
//    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();
//    private final AtomicLong ultimoHeartbeat = new AtomicLong(0);
//    private final Map<String, Boolean> cacheQueryConsolidable = new ConcurrentHashMap<>();
//    private final AtomicInteger cambiosEstrategiaPorOOM = new AtomicInteger(0);
//
//    private static final long HEARTBEAT_INTERVAL_MS = 30000;
//
//    private long tiempoInicioGlobal;
//
//    @Autowired
//    private QueryStorageRepository queryStorageRepository;
//
//    @Autowired
//    public void init() {
//        this.parallelExecutor = new ThreadPoolExecutor(
//                threadPoolSize,
//                threadPoolSize,
//                60L,
//                TimeUnit.SECONDS,
//                new LinkedBlockingQueue<>(100),
//                new ThreadPoolExecutor.CallerRunsPolicy());
//    }
//
//    @PreDestroy
//    public void shutdown() {
//        log.info("Cerrando BatchProcessor...");
//
//        // 1. CERRAR EXECUTOR
//        if (parallelExecutor != null && !parallelExecutor.isShutdown()) {
//            parallelExecutor.shutdown();
//            try {
//                if (!parallelExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
//                    log.warn("⏱️ Executor no terminó en 60s, forzando shutdown");
//                    parallelExecutor.shutdownNow();
//
//                    // Dar 30s más para terminar forzadamente
//                    if (!parallelExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
//                        log.error("❌ Executor no se pudo cerrar completamente");
//                    }
//                } else {
//                    log.info("✅ Executor cerrado correctamente");
//                }
//            } catch (InterruptedException e) {
//                log.warn("⚠️ Shutdown interrumpido, forzando cierre");
//                parallelExecutor.shutdownNow();
//                Thread.currentThread().interrupt();
//            }
//        }
//
//        // 2. LIMPIAR CACHES
//        cacheQueryConsolidable.clear();
//
//        // 3. REPORTAR MÉTRICAS (SIEMPRE, al final)
//        reportarMetricasFinales();
//
//
//
//        log.info("═══════════════════════════════════════════════════════════");
//        log.info("📊 PROGRESO PARALELO: {}/{} provincias ({:.1f}%)",
//                completadas, total, progreso);
//        log.info("   En proceso: {} | Registros: {} | Memoria: {:.1f}%",
//                enProceso, registrosActuales, obtenerPorcentajeMemoriaUsada());
//
//        // Mostrar estado detallado
//        estados.entrySet().stream()
//                .sorted(Map.Entry.comparingByKey())
//                .forEach(entry ->
//                        log.info("   {} - {}", entry.getKey(), entry.getValue())
//                );
//
//        log.info("═══════════════════════════════════════════════════════════");
//    }
//
//    private void procesarHibrido(
//            List<InfraccionesRepositoryImpl> repositories,
//            ParametrosFiltrosDTO filtros,
//            String nombreQuery,
//            ContextoProcesamiento contexto,
//            QueryStorage queryStorage) {
//
//        log.info("Ejecutando modo HÍBRIDO");
//
//        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
//            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
//            List<InfraccionesRepositoryImpl> grupo = repositories.subList(i, endIndex);
//
//            List<CompletableFuture<Void>> futures = grupo.stream()
//                    .map(repo -> CompletableFuture.runAsync(
//                            () -> ejecutarProvincia(repo, filtros, nombreQuery, contexto,queryStorage),
//                            parallelExecutor))
//                    .collect(Collectors.toList());
//
//            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//            contexto.procesarTodosResultados();
//
//            if (endIndex < repositories.size() && esMemoriaAlta()) {
//                pausarSiNecesario();
//            }
//        }
//    }
//
//    private void procesarSecuencial(
//            List<InfraccionesRepositoryImpl> repositories,
//            ParametrosFiltrosDTO filtros,
//            String nombreQuery,
//            ContextoProcesamiento contexto) {
//
//        int batchSize = calcularTamanoLoteOptimo(filtros);
//
//        for (int i = 0; i < repositories.size(); i++) {
//            InfraccionesRepositoryImpl repo = repositories.get(i);
//
//            logHeartbeat(repositories.size());
//            procesarProvinciaSecuencial(repo, filtros, nombreQuery, contexto, batchSize);
//            contexto.procesarTodosResultados();
//
//            if (i < repositories.size() - 1 && esMemoriaAlta()) {
//                pausarSiNecesario();
//            }
//        }
//    }
//
//    private void procesarProvinciaSecuencial(
//            InfraccionesRepositoryImpl repo,
//            ParametrosFiltrosDTO filtros,
//            String nombreQuery,
//            ContextoProcesamiento contexto,
//            int batchSize) {
//
//        String provincia = repo.getProvincia();
//        lastKeyPerProvince.remove(provincia);
//
//        int procesados = 0;
//        int offset = 0;
//        boolean continuar = true;
//
//        while (continuar) {
//            try {
//                if (esMemoriaCritica()) {
//                    batchSize = Math.max(500, batchSize / 2);
//                }
//
//                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset, provincia);
//                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);
//
//                if (lote == null || lote.isEmpty()) {
//                    break;
//                }
//
//                guardarLastKey(lote, provincia);
//
//                List<Map<String, Object>> loteInmutable = crearCopiasInmutables(lote, provincia);
//                contexto.agregarResultados(loteInmutable);
//
//                procesados += lote.size();
//                offset += batchSize;
//                continuar = lote.size() >= batchSize;
//
//            } catch (OutOfMemoryError oom) {
//                log.error("OOM en {}", provincia);
//                pausarSiNecesario();
//                break;
//            } catch (Exception e) {
//                log.error("Error en {}: {}", provincia, e.getMessage());
//                break;
//            }
//        }
//
//        actualizarContadores(provincia, procesados);
//        lastKeyPerProvince.remove(provincia);
//    }
//
//   /**
// * Ejecuta una provincia decidiendo la estrategia según el tipo de query.
// *
// * @param repo Repositorio de la provincia
// * @param filtros Filtros aplicados
// * @param nombreQuery Código de la query
// * @param contexto Contexto de procesamiento
// */
//private void ejecutarProvincia(
//        InfraccionesRepositoryImpl repo,
//        ParametrosFiltrosDTO filtros,
//        String nombreQuery,
//        ContextoProcesamiento contexto,
//        QueryStorage queryStorage) {
//
//    String provincia = repo.getProvincia();
//
//
//    // Verificar si debe forzar paginación (override manual)
//      boolean estrategiaSinPaginacion = queryStorage.getEstrategiaPaginacion() == EstrategiaPaginacion.SIN_PAGINACION;
//
//     // Si la query no es consolidable, siempre usar paginación
//    if (estrategiaSinPaginacion && esQueryConsolidable(nombreQuery)) {
//
//        // Decidir estrategia según tipo de consolidación
//        if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.AGREGACION) {
//            // Query bien diseñada (GROUP BY con pocos registros): carga directa
//            ejecutarQueryConsolidableAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
//
//        } else if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.CRUDO) {
//            // Query mal diseñada (sin GROUP BY o muchos registros): streaming
//            ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
//
//        } else if (queryStorage.getTipoConsolidacion() == TipoConsolidacion.DEDUPLICACION) {
//            // Query con deduplicación: carga directa si < 10K, sino streaming
//            if (queryStorage.getRegistrosEstimados() != null && queryStorage.getRegistrosEstimados() < 10000) {
//                ejecutarQueryConsolidableAgregacion(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
//            } else {
//                ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
//            }
//
//        } else {
//            // Tipo desconocido, JERARQUICA o COMBINADA: usar estrategia conservadora (streaming)
//            log.warn("Tipo de consolidación {} para {}, usando streaming",
//                     queryStorage.getTipoConsolidacion(), nombreQuery);
//            ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, queryStorage);
//        }
//    } else {
//        // Query normal: usar paginación estándar
//        ejecutarQueryPaginada(repo, filtros, nombreQuery, provincia, contexto);
//    }
//}
//
//    private boolean esQueryConsolidable(String nombreQuery) {
//        return cacheQueryConsolidable.computeIfAbsent(nombreQuery, key -> {
//            try {
//                Optional<QueryStorage> qs = queryRegistryService.buscarQuery(key);
//                return qs.map(QueryStorage::getEsConsolidable).orElse(false);
//            } catch (Exception e) {
//                return false;
//            }
//        });
//    }
//
//
//    /**
//     * 🛡️ VERSIÓN PROTEGIDA contra OOM
//     *
//     * Ejecuta query consolidable AGREGACION con validación de tamaño.
//     * Si detecta que el resultado es mucho mayor que lo estimado,
//     * cambia automáticamente a estrategia CRUDO (streaming).
//     */
//    private void ejecutarQueryConsolidableAgregacion(
//            InfraccionesRepositoryImpl repo,
//            ParametrosFiltrosDTO filtros,
//            String nombreQuery,
//            String provincia,
//            ContextoProcesamiento contexto,
//            QueryStorage info) {
//
//        try {
//            log.debug("🔍 Ejecutando query AGREGACION para {}: {} registros estimados",
//                    provincia, info.getRegistrosEstimados());
//
//            // ===== CAPA 1: VALIDACIÓN PRE-EJECUCIÓN =====
//            Integer estimacion = info.getRegistrosEstimados();
//
//            if (estimacion == null || estimacion == 0) {
//                log.warn("⚠️ Sin estimación confiable para query {}, usando estrategia CRUDO por seguridad",
//                        nombreQuery);
//                ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, info);
//                return;
//            }
//
//            // Si la estimación es muy alta, ir directo a CRUDO
//            if (estimacion > limiteValidacion) {
//                log.info("📊 Estimación alta ({} registros), usando estrategia CRUDO directamente",
//                        estimacion);
//                ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, info);
//                return;
//            }
//
//            // ===== CAPA 2: EJECUCIÓN CON LÍMITE DE VALIDACIÓN =====
//
//            // Crear filtros con límite de seguridad para la primera validación
//            ParametrosFiltrosDTO filtrosValidacion = filtros.toBuilder()
//                    .limite(limiteValidacion)  // Límite temporal para validar
//                    .offset(null)
//                    .build();
//
//            log.debug("🧪 Validando con límite de {} registros", limiteValidacion);
//
//            List<Map<String, Object>> muestraValidacion =
//                    repo.ejecutarQueryConFiltros(nombreQuery, filtrosValidacion);
//
//            if (muestraValidacion == null) {
//                log.warn("⚠️ Query retornó null para {}", provincia);
//                return;
//            }
//
//            int tamanoMuestra = muestraValidacion.size();
//
//            // ===== CAPA 3: ANÁLISIS Y DECISIÓN =====
//
//            // CASO 1: La muestra está incompleta (hay más datos)
//            if (tamanoMuestra >= limiteValidacion) {
//                log.warn("⚠️ Query retornó {} registros (límite de validación alcanzado)", tamanoMuestra);
//                log.warn("📊 Estimación era {} pero hay al menos {}+ registros", estimacion, limiteValidacion);
//
//                // Verificar si supera el umbral de error
//                if (tamanoMuestra > estimacion * umbralErrorEstimacion) {
//                    log.error("❌ ERROR DE ESTIMACIÓN DETECTADO: {}x más registros de lo estimado",
//                            tamanoMuestra / Math.max(estimacion, 1));
//                    log.info("🔄 Cambiando automáticamente a estrategia CRUDO (streaming)");
//
//                    // Actualizar estimación en base de datos para futuras ejecuciones
//                    actualizarEstimacionQuery(info, tamanoMuestra * 2);
//
//                    // Cambiar a estrategia segura
//                    ejecutarQueryConsolidableCrudo(repo, filtros, nombreQuery, provincia, contexto, info);
//                    return;
//                }
//
//                // Si está dentro del umbral pero es grande, usar paginación
//                log.info("📄 Usando paginación para procesar todos los registros");
//                ejecutarConPaginacionSegura(repo, filtros, nombreQuery, provincia, contexto, limiteValidacion);
//                return;
//            }
//
//            // CASO 2: La muestra es completa (caben todos los datos)
//            log.debug("✅ Muestra completa: {} registros (dentro de límite)", tamanoMuestra);
//
//            // Verificar si la diferencia con la estimación es razonable
//            if (estimacion > 0 && tamanoMuestra > estimacion * umbralErrorEstimacion) {
//                log.warn("⚠️ Discrepancia con estimación: esperados={}, reales={}",
//                        estimacion, tamanoMuestra);
//
//                // Actualizar estimación para la próxima vez
//                actualizarEstimacionQuery(info, tamanoMuestra);
//            }
//
//            // ===== PROCESAMIENTO NORMAL =====
//
//            if (tamanoMuestra == 0) {
//                log.debug("📭 Query no retornó resultados para {}", provincia);
//                return;
//            }
//
//            // Procesar resultados (ya están todos en memoria y son manejables)
//            List<Map<String, Object>> resultadoInmutable = crearCopiasInmutables(muestraValidacion, provincia);
//            contexto.agregarResultados(resultadoInmutable);
//            actualizarContadores(provincia, tamanoMuestra);
//
//            log.info("✅ Query AGREGACION completada para {}: {} registros | Memoria: {:.1f}%",
//                    provincia, tamanoMuestra, obtenerPorcentajeMemoriaUsada());
//
//        } catch (OutOfMemoryError oom) {
//            log.error("💥 OOM en query AGREGACION para {}. Esto NO debería pasar con las validaciones.",
//                    provincia);
//            log.error("🔧 Considera reducir consolidacion.agregacion.limite-validacion en properties");
//
//            // Intentar recuperación
//            System.gc();
//            throw new RuntimeException("OutOfMemoryError en consolidación AGREGACION", oom);
//
//        } catch (Exception e) {
//            log.error("❌ Error en query AGREGACION {} para {}: {}",
//                    nombreQuery, provincia, e.getMessage(), e);
//        }
//    }
//
//    /**
//     * 🔄 Ejecuta query con paginación segura cuando el resultado es grande pero manejable.
//     *
//     * Usado cuando la validación detecta que hay más de 10K registros pero menos de 100K.
//     */
//    private void ejecutarConPaginacionSegura(
//            InfraccionesRepositoryImpl repo,
//            ParametrosFiltrosDTO filtros,
//            String nombreQuery,
//            String provincia,
//            ContextoProcesamiento contexto,
//            int tamanoPagina) {
//
//        log.info("📄 Iniciando paginación segura para {}: páginas de {} registros",
//                provincia, tamanoPagina);
//
//        int offset = 0;
//        int totalProcesados = 0;
//        int iteracion = 0;
//        int maxIteraciones = limiteAbsoluto / tamanoPagina; // Límite de seguridad
//
//        while (iteracion < maxIteraciones) {
//            try {
//                // Crear filtros para esta página
//                ParametrosFiltrosDTO filtrosPagina = filtros.toBuilder()
//                        .limite(tamanoPagina)
//                        .offset(offset)
//                        .build();
//
//                // Ejecutar query
//                List<Map<String, Object>> pagina =
//                        repo.ejecutarQueryConFiltros(nombreQuery, filtrosPagina);
//
//                // Verificar si terminamos
//                if (pagina == null || pagina.isEmpty()) {
//                    log.debug("✅ Paginación completada en iteración {}", iteracion);
//                    break;
//                }
//
//                // Procesar página
//                List<Map<String, Object>> paginaInmutable = crearCopiasInmutables(pagina, provincia);
//                contexto.agregarResultados(paginaInmutable);
//
//                totalProcesados += pagina.size();
//                offset += tamanoPagina;
//                iteracion++;
//
//                log.debug("📄 Página {}: {} registros (total: {})", iteracion, pagina.size(), totalProcesados);
//
//                // Si la página no está llena, es la última
//                if (pagina.size() < tamanoPagina) {
//                    break;
//                }
//
//                // Pausa si la memoria está alta
//                if (esMemoriaAlta()) {
//                    pausarSiNecesario();
//                }
//
//            } catch (Exception e) {
//                log.error("❌ Error en iteración {} de paginación: {}", iteracion, e.getMessage());
//                break;
//            }
//        }
//
//        if (iteracion >= maxIteraciones) {
//            log.error("⚠️ Límite de iteraciones alcanzado ({}) para {}. Posible loop infinito evitado.",
//                    maxIteraciones, provincia);
//        }
//
//        actualizarContadores(provincia, totalProcesados);
//        log.info("✅ Paginación segura completada para {}: {} registros en {} páginas",
//                provincia, totalProcesados, iteracion);
//    }
//
//    /**
//     * 📊 Actualiza la estimación de registros en QueryStorage.
//     *
//     * Cuando detectamos que la estimación estaba incorrecta,
//     * actualizamos el registro en BD para mejorar futuras ejecuciones.
//     */
//    private void actualizarEstimacionQuery(QueryStorage info, int registrosReales) {
//        try {
//            log.info("🔄 Actualizando estimación para query {}: {} → {}",
//                    info.getCodigo(), info.getRegistrosEstimados(), registrosReales);
//
//            info.setRegistrosEstimados(registrosReales);
//            queryStorageRepository.save(info);
//
//            log.debug("✅ Estimación actualizada en base de datos");
//
//        } catch (Exception e) {
//            log.warn("⚠️ Error actualizando estimación para {}: {}",
//                    info.getCodigo(), e.getMessage());
//            // No fallar por esto, es solo metadata
//        }
//    }
//
//
//
///**
// * Ejecuta query consolidable CRUDO (mal diseñada).
// * La query retorna datos sin agregar y puede tener muchos registros.
// * Usa streaming para evitar OutOfMemoryError y consolida en memoria.
// *
// * Ejemplo: SELECT fecha, tipo, monto FROM infracciones WHERE...
// * Retorna: Miles de registros que deben ser consolidados
// *
// * @param repo Repositorio de la provincia
// * @param filtros Filtros aplicados
// * @param nombreQuery Código de la query
// * @param provincia Nombre de la provincia
// * @param contexto Contexto de procesamiento
// * @param info Información de la query
// */
//private void ejecutarQueryConsolidableCrudo(
//        InfraccionesRepositoryImpl repo,
//        ParametrosFiltrosDTO filtros,
//        String nombreQuery,
//        String provincia,
//        ContextoProcesamiento contexto,
//        QueryStorage info) {
//
//    try {
//        log.info("Ejecutando query consolidable CRUDO con streaming para {}: {} registros estimados",
//                 provincia, info.getRegistrosEstimados());
//
//        // Configurar para usar streaming
//        ParametrosFiltrosDTO filtrosStreaming = filtros.toBuilder()
//                .limite(null)
//                .offset(null)
//                .build();
//
//        // Buffer para procesar en chunks
//        final int CHUNK_SIZE = 1000;
//        final List<Map<String, Object>> buffer = new ArrayList<>(CHUNK_SIZE);
//        final AtomicInteger totalProcesados = new AtomicInteger(0);
//        final AtomicInteger chunksEnviados = new AtomicInteger(0);
//
//        // Callback que se ejecuta por cada registro
//        Consumer<Map<String, Object>> procesarRegistro = registro -> {
//            buffer.add(registro);
//
//            // Cuando el buffer se llena, procesarlo
//            if (buffer.size() >= CHUNK_SIZE) {
//                procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
//            }
//        };
//
//        // Ejecutar query con streaming
//        repo.ejecutarQueryConStreaming(nombreQuery, filtrosStreaming, procesarRegistro);
//
//        // Procesar registros restantes en el buffer
//        if (!buffer.isEmpty()) {
//            procesarChunk(buffer, provincia, contexto, totalProcesados, chunksEnviados);
//        }
//
//        // Actualizar contador global
//        actualizarContadores(provincia, totalProcesados.get());
//
//        log.info(" Query consolidable CRUDO completada para {}: {} registros en {} chunks | Memoria: {:.1f}%",
//                 provincia,
//                 totalProcesados.get(),
//                 chunksEnviados.get(),
//                 obtenerPorcentajeMemoriaUsada());
//
//    } catch (Exception e) {
//        log.error(" Error en query consolidable CRUDO para {}: {}", provincia, e.getMessage(), e);
//    }
//}
//
///**
// * Procesa un chunk de registros y lo envía al contexto.
// * Libera memoria después de procesar.
// *
// * @param buffer Buffer con registros a procesar
// * @param provincia Nombre de la provincia
// * @param contexto Contexto de procesamiento
// * @param totalProcesados Contador total de registros procesados
// * @param chunksEnviados Contador de chunks enviados
// */
//private void procesarChunk(
//        List<Map<String, Object>> buffer,
//        String provincia,
//        ContextoProcesamiento contexto,
//        AtomicInteger totalProcesados,
//        AtomicInteger chunksEnviados) {
//
//    // SOLO procesar si el buffer NO está vacío
//    if(!buffer.isEmpty()){
//
//        // Crear copia inmutable del chunk
//        List<Map<String, Object>> chunkInmutable = crearCopiasInmutables(buffer, provincia);
//

//        int iteracion = 0;
//        final int batchSize = 10000;  // ✅ Límite alto para consolidadas
//        int offset = 0;  // ✅ Usar OFFSET en lugar de lastId
//
//        Integer estimacion = obtenerEstimacionProvincia(nombreQuery, provinciaRepo);
//
//        log.info("🔄 {} - Iniciando paginación OFFSET (estimado: {} registros)",
//                provinciaRepo, estimacion != null ? estimacion : "desconocido");
//
//        while (true) {
//            try {
//                // ✅ Log ANTES
//                log.info("🔹 {} - Iteración {}: OFFSET={}, procesados: {}/{}",
//                        provinciaRepo, iteracion, offset, procesados,
//                        estimacion != null ? estimacion : "?");
//
//                long inicioIteracion = System.currentTimeMillis();
//
//                // ✅ Crear filtros con OFFSET
//                ParametrosFiltrosDTO filtrosLote = filtros.toBuilder()
//                        .limite(batchSize)
//                        .offset(offset)      // ✅ CAMBIO: Usar OFFSET
//                        .lastId(null)        // ✅ CAMBIO: No usar lastId
//                        .lastSerieEquipo(null)
//                        .lastLugar(null)
//                        .lastKeysetConsolidacion(null)
//                        .build();
//
//                // Ejecutar query
//                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);
//
//                long duracionIteracion = System.currentTimeMillis() - inicioIteracion;
//
//                // ✅ Log DESPUÉS
//                log.info("✅ {} - Iteración {}: Recibidos {} registros en {}ms",
//                        provinciaRepo, iteracion,
//                        lote != null ? lote.size() : 0,
//                        duracionIteracion);
//
//                // Verificar si hay datos
//                if (lote == null || lote.isEmpty()) {
//                    log.info("🏁 {} - Fin de datos en iteración {}", provinciaRepo, iteracion);
//                    break;
//                }
//
//                // ✅ CAMBIO: Ya no necesitas extraer lastId
//
//                // Procesar resultados (sin incluir row_id en resultados finales)
//                List<Map<String, Object>> loteInmutable = crearCopiasInmutablesLimpio(lote, provinciaRepo);
//                contexto.agregarResultados(loteInmutable);
//
//                actualizarContadores(provinciaRepo,lote.size());
//
//                // Actualizar contadores
//                procesados += lote.size();
//                offset += batchSize;  // ✅ CAMBIO: Incrementar offset en lugar de lastId
//                iteracion++;
//
//                // ✅ Log de progreso
//                if (estimacion != null && estimacion > 0) {
//                    double progreso = (double) procesados / estimacion * 100;
//                    log.info("📊 {} - Progreso: {}/{} registros ({:.1f}%) en {} iteraciones",
//                            provinciaRepo, procesados, estimacion, progreso, iteracion);
//                }
//
//                actualizarContadores(provinciaRepo,lote.size());
//
//                // ✅ Condición de salida: lote incompleto
//                if (lote.size() < batchSize) {
//                    log.info("🏁 {} - Última página (lote incompleto: {})",
//                            provinciaRepo, lote.size());
//                    break;
//                }
//
//                // Límite de seguridad
//                if (iteracion >= 100) {
//                    log.warn("⚠️ {} - Límite de iteraciones alcanzado", provinciaRepo);
//                    break;
//                }
//
//                if (esMemoriaAlta()) {
//                    pausarSiNecesario();
//                }
//
//            } catch (Exception e) {
//                log.error("❌ {} - Error en iteración {}: {}",
//                        provinciaRepo, iteracion, e.getMessage(), e);
//                break;
//            }
//        }
//
//        log.info("✅ {} - Paginación completada: {} registros en {} iteraciones | Memoria: {:.1f}%",
//                provinciaRepo, procesados, iteracion, obtenerPorcentajeMemoriaUsada());
//
//        actualizarContadores(provinciaRepo, procesados);
//    }
//
//
//
//    /**
//     * Convierte un Object a Integer de forma segura.
//     */
//    private Integer convertirAInteger(Object valor) {
//        if (valor instanceof Integer) {
//            return (Integer) valor;
//        } else if (valor instanceof Long) {
//            return ((Long) valor).intValue();
//        } else if (valor instanceof BigDecimal) {
//            return ((BigDecimal) valor).intValue();
//        } else if (valor instanceof String) {
//            return Integer.parseInt((String) valor);
//        } else {
//            throw new IllegalArgumentException("Tipo no soportado: " + valor.getClass());
//        }
//    }
//
//    /**
//     * Crea copias inmutables SIN incluir row_id (campo técnico).
//     *
//     * @param registros Lista de registros
//     * @param provincia Nombre de la provincia
//     * @return Lista de copias limpias
//     */
//    private List<Map<String, Object>> crearCopiasInmutablesLimpio(
//            List<Map<String, Object>> registros,
//            String provincia) {
//
//        return registros.stream()
//                .map(registro -> {
//                    Map<String, Object> copia = new HashMap<>();
//
//                    // Copiar todos los campos EXCEPTO row_id
//                    registro.entrySet().stream()
//                            .filter(e -> !"row_id".equals(e.getKey()))  // ✅ Excluir row_id
//                            .filter(e -> !"provincia".equals(e.getKey()))
//                            .forEach(e -> copia.put(e.getKey(), e.getValue()));
//
//                    // Agregar provincia
//                    copia.put("provincia", provincia);
//
//                    return copia;
//                })
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * Obtiene estimación de registros para una provincia.
//     */
//    private Integer obtenerEstimacionProvincia(String nombreQuery, String provincia) {
//        try {
//            return queryRegistryService.buscarQuery(nombreQuery)
//                    .map(QueryStorage::getRegistrosEstimados)
//                    .orElse(null);
//        } catch (Exception e) {
//            return null;
//        }
//    }
//
//    /**
//     * Obtiene el último ID del lote actual
//     */
//    @Deprecated
//    private Integer obtenerUltimoIdDelLote(List<Map<String, Object>> lote) {
//        if (lote == null || lote.isEmpty()) {
//            return null;
//        }
//
//        Map<String, Object> ultimoRegistro = lote.get(lote.size() - 1);
//
//        // Buscar campo ID con diferentes nombres posibles
//        String[] posiblesCamposId = {"id", "infraccion_id", "registro_id", "consecutivo"};
//
//        for (String campo : posiblesCamposId) {
//            Object valor = ultimoRegistro.get(campo);
//            if (valor instanceof Integer) {
//                return (Integer) valor;
//            } else if (valor instanceof Long) {
//                return ((Long) valor).intValue();
//            } else if (valor instanceof String) {
//                try {
//                    return Integer.parseInt((String) valor);
//                } catch (NumberFormatException e) {
//                    // Continuar con siguiente campo
//                }
//            }
//        }
//
//        log.warn("⚠️ No se encontró campo ID válido en el último registro. Campos disponibles: {}",
//                ultimoRegistro.keySet());
//        return null;
//    }
//
//
//    private List<Map<String, Object>> crearCopiasInmutables(List<Map<String, Object>> registros, String provincia) {
//        return registros.stream()
//                .map(registro -> {
//                    Map<String, Object> copia = new HashMap<>();
//                    registro.entrySet().stream()
//                            .filter(e -> !"provincia".equals(e.getKey()))
//                            .forEach(e -> copia.put(e.getKey(), e.getValue()));
//                    copia.put("provincia", provincia);
//                    return copia;
//                })
//                .collect(Collectors.toList());
//    }
//
//    private void actualizarContadores(String provincia, int cantidad) {
//        // ✅ SUMAR al contador de la provincia (no reemplazar)
//        contadoresPorProvincia.merge(provincia, cantidad, Integer::sum);
//
//        // ✅ SUMAR al contador global
//        totalRegistrosGlobales.addAndGet(cantidad);
//
//        log.debug("📊 Contadores actualizados - {}: +{} → Total provincia: {}, Total global: {}",
//                provincia,
//                cantidad,
//                contadoresPorProvincia.get(provincia),
//                totalRegistrosGlobales.get());
//    }
//
//    /**
//     * MÉTODO CORREGIDO: Guarda lastKey detectando el tipo correcto
//     *
//     * PROBLEMA ANTERIOR:
//     * - Asumía que lastKey[0] siempre era Integer
//     * - Queries de consolidación NO tienen campo 'id'
//     *
//     * SOLUCIÓN:
//     * - Guardar los valores tal como vienen
//     * - NO asumir tipos en guardarLastKey
//     * - Validar tipos en crearFiltrosParaLoteSiguiente
//     */
//    private void guardarLastKey(List<Map<String, Object>> lote, String provincia) {
//        if (lote == null || lote.isEmpty()) {
//            return;
//        }
//
//        try {
//            Map<String, Object> ultimo = lote.get(lote.size() - 1);
//
//            // CASO 1: Query estándar con campo 'id'
//            if (ultimo.containsKey("id") && ultimo.get("id") != null) {
//                Object id = ultimo.get("id");
//
//                lastKeyPerProvince.put(provincia, new Object[] {
//                        id,
//                        ultimo.get("serie_equipo"),
//                        ultimo.get("lugar")
//                });
//
//                log.debug("🔑 Keyset estándar guardado para {}: id={} (tipo: {})",
//                        provincia, id, id.getClass().getSimpleName());
//            }
//            // CASO 2: Query de consolidación SIN campo 'id'
//            else {
//                // Tomar los primeros 3 campos disponibles (sin asumir tipos)
//                List<Object> keyValues = new ArrayList<>();
//                int count = 0;
//
//                for (Map.Entry<String, Object> entry : ultimo.entrySet()) {
//                    if (entry.getValue() != null && count < 3) {
//                        keyValues.add(entry.getValue());
//                        count++;
//                    }
//                }
//
//                if (!keyValues.isEmpty()) {
//                    lastKeyPerProvince.put(provincia, keyValues.toArray());
//
//                    log.debug("🔑 Keyset consolidación guardado para {}: {} campos (tipos: {})",
//                            provincia,
//                            keyValues.size(),
//                            keyValues.stream()
//                                    .map(v -> v.getClass().getSimpleName())
//                                    .collect(Collectors.joining(", ")));
//                }
//            }
//        } catch (Exception e) {
//            log.warn("⚠️ Error guardando lastKey para {}: {}", provincia, e.getMessage());
//        }
//    }
//
//   private void logHeartbeat(int totalRepositorios) {
//    long ahora = System.currentTimeMillis();
//    long ultimo = ultimoHeartbeat.get();
//
//    if (ahora - ultimo > HEARTBEAT_INTERVAL_MS) {
//        // Actualizar sin condición de carrera
//        ultimoHeartbeat.set(ahora);
//        log.info("Heartbeat - {}s | {} registros | Memoria: {:.1f}%",
//                (ahora - tiempoInicioGlobal) / 1000,
//                totalRegistrosGlobales.get(),
//                obtenerPorcentajeMemoriaUsada());
//    }
//}
//
//    private double obtenerPorcentajeMemoriaUsada() {
//        Runtime runtime = Runtime.getRuntime();
//        return (double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory() * 100;
//    }
//
//    private boolean esMemoriaCritica() {
//        Runtime runtime = Runtime.getRuntime();
//        return ((double) (runtime.totalMemory() - runtime.freeMemory())
//                / runtime.maxMemory()) > memoryCriticalThreshold;
//    }
//
//    private boolean esMemoriaAlta() {
//        Runtime runtime = Runtime.getRuntime();
//        return ((double) (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory()) > 0.70;
//    }
//
//    private void pausarSiNecesario() {
//        if (esMemoriaAlta()) {
//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//    }
//
//    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
//        int batchSizeBase = filtros.getLimiteEfectivo();
//        if (batchSizeBase < 1000) {
//            batchSizeBase = defaultBatchSize;
//        }
//
//        Runtime runtime = Runtime.getRuntime();
//        long memoriaLibre = runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());
//        double porcentajeLibre = (double) memoriaLibre / runtime.maxMemory();
//
//        if (porcentajeLibre < 0.20) {
//            return Math.max(1000, batchSizeBase / 4);
//        } else if (porcentajeLibre < 0.30) {
//            return Math.max(2000, batchSizeBase / 2);
//        }
//        return Math.min(batchSizeBase, 10000);
//    }
//
//    /**
//     * MÉTODO CORREGIDO: crearFiltrosParaLote
//     *
//     * CAMBIOS:
//     * 1. Primera iteración YA usa límite razonable (no Integer.MAX_VALUE)
//     * 2. Keyset se activa desde la SEGUNDA iteración
//     * 3. NUNCA usa offset (solo keyset)
//     */
//    private ParametrosFiltrosDTO crearFiltrosParaLote(
//            ParametrosFiltrosDTO filtrosOriginales,
//            int batchSize,
//            int offset,
//            String provincia) {
//
//        // PRIMERA ITERACIÓN: offset=0, sin keyset aún
//        if (offset == 0 || !lastKeyPerProvince.containsKey(provincia)) {
//            log.debug("🔹 Primera iteración para {}: limite={}, keyset=INACTIVO", provincia, batchSize);
//
//            return filtrosOriginales.toBuilder()
//                    .limite(batchSize)  // CRÍTICO: Usar límite razonable
//                    .offset(null)       // NUNCA usar offset
//                    .lastId(null)
//                    .lastSerieEquipo(null)
//                    .lastLugar(null)
//                    .lastKeysetConsolidacion(null)
//                    .build();
//        }
//
//        // ITERACIONES SIGUIENTES: usar keyset
//        Object[] lastKey = lastKeyPerProvince.get(provincia);
//
//        // Detectar tipo de keyset
//        if (esKeysetEstandar(lastKey)) {
//            // Keyset estándar (id, serie_equipo, lugar)
//            log.debug("🔑 Keyset estándar para {}: id={}", provincia, lastKey[0]);
//
//            return filtrosOriginales.toBuilder()
//                    .limite(batchSize)
//                    .offset(null)  // NUNCA usar offset con keyset
//                    .lastId((Integer) lastKey[0])
//                    .lastSerieEquipo(lastKey.length > 1 ? (String) lastKey[1] : null)
//                    .lastLugar(lastKey.length > 2 ? (String) lastKey[2] : null)
//                    .build();
//
//        } else {
//            // Keyset consolidación genérico
//            Map<String, Object> keysetMap = construirKeysetMap(lastKey);
//
//            log.debug("🔑 Keyset consolidación para {}: {} campos", provincia, keysetMap.size());
//
//            return filtrosOriginales.toBuilder()
//                    .limite(batchSize)
//                    .offset(null)
//                    .lastKeysetConsolidacion(keysetMap)
//                    .build();
//        }
//    }
//
//    /**
//     * Detecta si es keyset estándar (con id numérico)
//     */
//    private boolean esKeysetEstandar(Object[] lastKey) {
//        return lastKey != null
//                && lastKey.length >= 1
//                && lastKey[0] instanceof Integer;
//    }
//
//    /**
//     * Construye mapa de keyset para queries de consolidación
//     */
//    private Map<String, Object> construirKeysetMap(Object[] lastKey) {
//        Map<String, Object> keysetMap = new HashMap<>();
//
//        for (int i = 0; i < Math.min(lastKey.length, 3); i++) {
//            if (lastKey[i] != null) {
//                keysetMap.put("campo_" + i, lastKey[i]);
//            }
//        }
//
//        return keysetMap;
//    }
//
//}