package org.transito_seguro.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.exception.RuntimesFormatoExceleException;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.model.query.QueryResult;

import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Servicio principal para gestión de consultas de infracciones.
 *
 * ARQUITECTURA LIMPIA:
 * - Delega toda la lógica de consolidación a ConsolidacionService
 * - Se enfoca en orquestación, validación y generación de respuestas
 * - Mantiene responsabilidad única: coordinar el flujo de ejecución
 *
 * @author Sistema Tránsito Seguro
 * @version 2.0 - Refactorizado con delegación completa
 */
@Slf4j
@Service
public class InfraccionesService {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private ConsultaValidator validator;

    @Autowired
    private FormatoConverter formatoConverter;

    @Autowired
    private BatchProcessor batchProcessor;

    /**
     * Servicio especializado que maneja TODA la lógica de consolidación.
     * InfraccionesService solo coordina y delega.
     */
    @Autowired
    private ConsolidacionService consolidacionService;

    @Autowired
    private StreamingFormatoConverter streamingConverter;

    @Autowired
    private QueryStorageRepository queryStorageRepository;

    @Autowired
    private ParametrosProcessor parametrosProcessor;

    // =============== CONFIGURACIÓN ===============

    @Value("${app.limits.max-records-sync:1000}")
    private int maxRecordsSincrono;

    @Value("${app.limits.max-records-total:50000}")
    private int maxRecordsTotal;

    @Value("${app.async.thread-pool-size:10}")
    private int threadPoolSize;

    private ExecutorService executor;

    // Cache para queries consolidables
    private final Map<String, Boolean> cacheQueryConsolidable = new ConcurrentHashMap<>();

    // =============== CONSTRUCTOR Y LIFECYCLE ===============

    /**
     * Inicializa el pool de threads para procesamiento paralelo.
     */
    @Autowired
    public void init() {
        this.executor = new ThreadPoolExecutor(
                Math.max(threadPoolSize, 10),
                Math.max(threadPoolSize, 10),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        log.info("InfraccionesService inicializado con pool de {} threads", threadPoolSize);
    }

    /**
     * Limpieza ordenada de recursos al cerrar la aplicación.
     */
    @PreDestroy
    public void cleanup() {
        log.info("Cerrando InfraccionesService...");
        cacheQueryConsolidable.clear();

        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        log.error("ExecutorService no se pudo cerrar correctamente");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("InfraccionesService cerrado exitosamente");
    }

    // =============== API PÚBLICA PRINCIPAL ===============

    /**
     * Ejecuta una consulta por tipo y retorna los resultados en el formato especificado.
     *
     * @param tipoConsulta Código de la query a ejecutar
     * @param consulta DTO con parámetros de filtros y formato
     * @return Resultados en el formato solicitado (JSON, CSV, Excel)
     * @throws ValidationException Si la consulta no es válida
     */
    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta)
            throws ValidationException {
        log.info("Ejecutando consulta: {}", tipoConsulta);
        validarTipoConsulta(tipoConsulta);
        return ejecutarQueryDesdeBaseDatos(tipoConsulta, consulta);
    }

    /**
     * Genera y descarga archivo con los resultados de la consulta.
     *
     * @param tipoConsulta Código de la query
     * @param consulta Parámetros de la consulta
     * @return ResponseEntity con el archivo generado
     * @throws ValidationException Si la consulta no es válida
     */
    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta)
            throws ValidationException {
        log.info("Descargando archivo: {}", tipoConsulta);
        validarTipoConsulta(tipoConsulta);
        return consultarInfraccionesComoArchivo(consulta, tipoConsulta);
    }

    /**
     * Determina qué repositorios (provincias) usar según los filtros.
     *
     * LÓGICA:
     * - Si consolidado=true → todas las provincias
     * - Si usarTodasLasBDS=true → todas las provincias
     * - Si se especifican bases de datos → solo esas provincias
     * - Por defecto → todas las provincias disponibles
     *
     * @param filtros Parámetros de filtrado
     * @return Lista de repositorios a consultar
     */
    public List<InfraccionesRepositoryImpl> determinarRepositories(ParametrosFiltrosDTO filtros) {
        // Caso 1: Consolidación activa - usar todos los repositorios
        if (filtros != null && filtros.esConsolidado()) {
            log.debug("Consolidación activa - usando todos los repositorios");
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }

        // Caso 2: Flag explícito para usar todas las BDs
        if (filtros != null && Boolean.TRUE.equals(filtros.getUsarTodasLasBDS())) {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }

        // Caso 3: Provincias específicas solicitadas
        if (filtros != null && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            List<String> provinciasNormalizadas = validator.normalizarProvincias(filtros.getBaseDatos());
            return provinciasNormalizadas.stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(provincia -> (InfraccionesRepositoryImpl) repositoryFactory.getRepository(provincia))
                    .collect(Collectors.toList());
        }

        // Caso 4: Por defecto, usar todos los repositorios
        return repositoryFactory.getAllRepositories().values().stream()
                .map(repo -> (InfraccionesRepositoryImpl) repo)
                .collect(Collectors.toList());
    }


    // =============== CORE: EJECUCIÓN DESDE BASE DE DATOS ===============

    /**
     * Método principal de ejecución de queries.
     *
     * FLUJO:
     * 1. Obtener y validar la query desde BD
     * 2. Registrar uso para estadísticas
     * 3. Validar parámetros de consulta
     * 4. Determinar repositorios a usar
     * 5. Ejecutar según tipo de procesamiento
     *
     * @param codigoQuery Código de la query
     * @param consulta Parámetros de la consulta
     * @return Resultados procesados
     * @throws ValidationException Si hay errores de validación
     */
    public Object ejecutarQueryDesdeBaseDatos(String codigoQuery, ConsultaQueryDTO consulta)
            throws ValidationException {

        log.info("Ejecutando query BD: {} - Consolidado: {}",
                codigoQuery, consulta.getParametrosFiltros() != null &&
                        consulta.getParametrosFiltros().esConsolidado());

        // PASO 1: Obtener metadata de la query
        QueryStorage queryStorage = obtenerYValidarQuery(codigoQuery);

        // PASO 2: Registrar uso para analytics
        registrarUsoQuery(queryStorage);

        // PASO 3: Validar parámetros
        validator.validarConsulta(consulta);

        // PASO 4: Determinar qué provincias consultar
        List<InfraccionesRepositoryImpl> repositories =
                determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No hay repositorios válidos para: {}", codigoQuery);
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // PASO 5: Ejecutar con la estrategia adecuada
        return ejecutarSegunTipoProcesamiento(queryStorage, repositories, consulta);
    }

    /**
     * Genera archivo descargable con los resultados de la consulta.
     *
     * CASOS:
     * - Consolidado → Usa ConsolidacionService y genera CSV
     * - Normal → Usa BatchProcessor y genera el formato solicitado
     *
     * @param consulta Parámetros de la consulta
     * @param nombreQuery Nombre de la query
     * @return ResponseEntity con archivo generado
     * @throws ValidationException Si hay errores
     */
    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(
            ConsultaQueryDTO consulta,
            String nombreQuery) throws ValidationException {

        log.info("Generando archivo para: {}", nombreQuery);
        validator.validarConsulta(consulta);

        List<InfraccionesRepositoryImpl> repositories =
                determinarRepositories(consulta.getParametrosFiltros());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        // ✅ DELEGACIÓN: Si es consolidado, usar ConsolidacionService
        if (consulta.getParametrosFiltros() != null &&
                consulta.getParametrosFiltros().esConsolidado()) {

            log.info("📊 Archivo consolidado CSV - {} provincias", repositories.size());
            return generarArchivoConsolidadoConServicio(repositories, consulta, nombreQuery);
        }

        // Para queries normales, usar procesamiento batch estándar
        log.info("📄 Archivo normal - {} repositorios, formato: {}", repositories.size(), formato);
        return generarArchivoNormal(repositories, consulta, nombreQuery, formato);
    }

    // =============== MÉTODOS PRIVADOS: VALIDACIÓN ===============

    /**
     * Valida que el tipo de consulta existe en la base de datos.
     *
     * @param tipoConsulta Código de la query
     * @throws ValidationException Si la query no existe
     */
    private void validarTipoConsulta(String tipoConsulta) throws ValidationException {
        List<String> nombresQuerys = queryStorageRepository.findAllbyCodigo();

        if (nombresQuerys.isEmpty()) {
            throw new ValidationException("No hay queries registradas en base de datos");
        }

        if (!nombresQuerys.contains(tipoConsulta)) {
            throw new ValidationException("Query '" + tipoConsulta + "' no existe. " +
                    "Disponibles: " + String.join(", ", nombresQuerys));
        }
    }

    /**
     * Obtiene la query desde BD y valida que esté lista para usar.
     *
     * @param codigoQuery Código de la query
     * @return QueryStorage con metadata
     * @throws IllegalArgumentException Si no se encuentra
     * @throws IllegalStateException Si no está lista
     */
    private QueryStorage obtenerYValidarQuery(String codigoQuery) {
        Optional<QueryStorage> queryStorageOpt = queryStorageRepository.findByCodigo(codigoQuery);

        if (!queryStorageOpt.isPresent()) {
            throw new IllegalArgumentException("Query no encontrada: " + codigoQuery);
        }

        QueryStorage queryStorage = queryStorageOpt.get();
        if (!queryStorage.estaLista()) {
            throw new IllegalStateException("Query no disponible: " + codigoQuery);
        }

        return queryStorage;
    }

    /**
     * Registra el uso de la query para estadísticas.
     *
     * @param queryStorage Metadata de la query
     */
    private void registrarUsoQuery(QueryStorage queryStorage) {
        try {
            queryStorage.registrarUso();
            queryStorageRepository.save(queryStorage);
            log.debug("Uso registrado: {}", queryStorage.getCodigo());
        } catch (Exception e) {
            log.warn("Error registrando uso de '{}': {}",
                    queryStorage.getCodigo(), e.getMessage());
        }
    }

    // =============== LÓGICA DE DECISIÓN DE ESTRATEGIA ===============

    /**
     * Decide qué estrategia de procesamiento usar según las características de la consulta.
     *
     * ESTRATEGIAS:
     * 1. Consolidación con lotes → Usa ConsolidacionService + BatchProcessor
     * 2. Consolidación secuencial → Usa ConsolidacionService directamente
     * 3. Lotes sin consolidar → Usa BatchProcessor
     * 4. Ejecución normal → Procesamiento paralelo estándar
     *
     * @param queryStorage Metadata de la query
     * @param repositories Repositorios a consultar
     * @param consulta Parámetros de la consulta
     * @return Resultados procesados
     * @throws ValidationException Si hay errores
     */
    private Object ejecutarSegunTipoProcesamiento(
            QueryStorage queryStorage,
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Determinar si es consolidado
        boolean esConsolidado = filtros != null &&
                filtros.esConsolidado() &&
                esQueryConsolidable(queryStorage);

        // Determinar si usar procesamiento por lotes
        boolean usarLotes = debeUsarLotes(filtros, repositories);

        log.info("📋 Estrategia seleccionada - Consolidado: {}, Lotes: {}",
                esConsolidado, usarLotes);

        // ✅ DELEGACIÓN COMPLETA: Todas las variantes consolidadas usan ConsolidacionService
        if (esConsolidado) {
            return ejecutarConsolidacion(queryStorage, repositories, consulta);
        }
        // Procesamiento sin consolidar
        else if (usarLotes) {
            return ejecutarConLotes(queryStorage, repositories, consulta);
        }
        else {
            return ejecutarQueryStorageNormal(queryStorage, repositories, consulta);
        }
    }

    /**
     * Verifica si una query puede ser consolidada (usa caché).
     *
     * @param queryStorage Metadata de la query
     * @return true si es consolidable
     */
    private boolean esQueryConsolidable(QueryStorage queryStorage) {
        return cacheQueryConsolidable.computeIfAbsent(
                queryStorage.getCodigo(),
                key -> queryStorage.getEsConsolidable()
        );
    }

    /**
     * Determina si se debe usar procesamiento por lotes.
     *
     * CRITERIOS:
     * - Flag "usarTodasLasBDS" activo
     * - Límite de registros > 25,000
     * - 4 o más repositorios
     * - Límite infinito (Integer.MAX_VALUE)
     *
     * @param filtros Parámetros de filtrado
     * @param repositories Lista de repositorios
     * @return true si debe usar lotes
     */
    private boolean debeUsarLotes(ParametrosFiltrosDTO filtros,
                                  List<InfraccionesRepositoryImpl> repositories) {
        if (filtros == null) return false;

        boolean todasLasBDS = Boolean.TRUE.equals(filtros.getUsarTodasLasBDS());
        int limiteEfectivo = filtros.getLimiteEfectivo();
        int numRepositorios = repositories.size();

        boolean resultado = todasLasBDS ||
                limiteEfectivo > 25000 ||
                numRepositorios >= 4 ||
                limiteEfectivo == Integer.MAX_VALUE;

        log.debug("Análisis lotes - TodasBDS: {}, Límite: {}, Repos: {}, Usar: {}",
                todasLasBDS, limiteEfectivo, numRepositorios, resultado);

        return resultado;
    }

    // =============== EJECUCIÓN: CONSOLIDACIÓN (DELEGADA) ===============

    /**
     * ✅ MÉTODO PRINCIPAL DE CONSOLIDACIÓN - 100% DELEGADO
     *
     * Delega TODA la lógica de consolidación a ConsolidacionService.
     * InfraccionesService solo coordina el flujo y convierte el formato final.
     *
     * RESPONSABILIDADES:
     * - InfraccionesService: Coordinar, validar, convertir formato
     * - ConsolidacionService: Consolidar datos (estrategias, agrupación, suma)
     *
     * @param queryStorage Metadata de la query
     * @param repositories Repositorios a consolidar
     * @param consulta Parámetros de la consulta
     * @return Datos consolidados en el formato solicitado
     */
    private Object ejecutarConsolidacion(
            QueryStorage queryStorage,
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta) {

        log.info("🎯 CONSOLIDACIÓN DELEGADA a ConsolidacionService");
        log.info("   Query: {}, Provincias: {}",
                queryStorage.getCodigo(), repositories.size());

        try {
            ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

            // ✅ DELEGACIÓN TOTAL: ConsolidacionService maneja toda la consolidación
            List<Map<String, Object>> datosConsolidados =
                    consolidacionService.consolidarDatos(
                            repositories,
                            queryStorage.getCodigo(),
                            filtros
                    );

            log.info("✅ Consolidación completada: {} grupos únicos", datosConsolidados.size());

            // Convertir al formato solicitado (JSON, CSV, Excel)
            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return formatoConverter.convertir(datosConsolidados, formato);

        } catch (Exception e) {
            log.error("❌ Error en consolidación: {}", e.getMessage(), e);
            throw new RuntimeException("Error ejecutando consolidación", e);
        }
    }

    // =============== EJECUCIÓN: LOTES SIN CONSOLIDAR ===============

    /**
     * Ejecuta query en múltiples repositorios usando procesamiento por lotes.
     * NO consolida datos, solo los combina.
     *
     * @param queryStorage Metadata de la query
     * @param repositories Repositorios a consultar
     * @param consulta Parámetros
     * @return Resultados combinados sin consolidar
     */
    private Object ejecutarConLotes(
            QueryStorage queryStorage,
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta) {

        log.info("📦 Procesamiento por LOTES sin consolidar");

        List<Map<String, Object>> resultados = Collections.synchronizedList(new ArrayList<>());

        // BatchProcessor maneja la paginación y procesamiento paralelo
        batchProcessor.procesarEnLotes(
                repositories,
                consulta.getParametrosFiltros(),
                queryStorage.getCodigo(),
                resultados::addAll  // Callback que acumula resultados
        );

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultados, formato);
    }

    // =============== EJECUCIÓN: NORMAL (PARALELA) ===============

    /**
     * Ejecución estándar para queries simples.
     * Usa procesamiento paralelo sin lotes ni consolidación.
     *
     * @param queryStorage Metadata de la query
     * @param repositories Repositorios a consultar
     * @param consulta Parámetros
     * @return Resultados combinados
     * @throws ValidationException Si hay errores
     */
    private Object ejecutarQueryStorageNormal(
            QueryStorage queryStorage,
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta) throws ValidationException {

        log.info("⚡ Ejecución PARALELA estándar");

        // Procesamiento paralelo usando streams
        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();
                        String sqlDinamico = queryStorage.getSqlQuery();

                        // Procesar query con parámetros dinámicos
                        QueryResult resultado = parametrosProcessor.procesarQuery(
                                sqlDinamico, consulta.getParametrosFiltros());

                        // Ejecutar query en esta provincia
                        List<Map<String, Object>> datos = repo.getNamedParameterJdbcTemplate()
                                .queryForList(
                                        resultado.getQueryModificada(),
                                        resultado.getParametros()
                                );

                        // Agregar metadata de provincia
                        datos.forEach(registro -> {
                            registro.put("provincia", provincia);
                            registro.put("query_codigo", queryStorage.getCodigo());
                        });

                        return datos.stream();

                    } catch (Exception e) {
                        log.error("Error ejecutando query '{}' en provincia '{}': {}",
                                queryStorage.getCodigo(), repo.getProvincia(), e.getMessage());
                        return java.util.stream.Stream.empty();
                    }
                })
                .collect(Collectors.toList());

        log.info("✅ Ejecución completada: {} registros de {} provincias",
                resultadosCombinados.size(), repositories.size());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    // =============== GENERACIÓN DE ARCHIVOS ===============

    /**
     * ✅ NUEVO: Genera archivo consolidado usando ConsolidacionService
     *
     * DELEGACIÓN COMPLETA:
     * 1. ConsolidacionService consolida los datos
     * 2. InfraccionesService genera el archivo CSV
     *
     * @param repositories Repositorios a consolidar
     * @param consulta Parámetros
     * @param nombreQuery Nombre de la query
     * @return ResponseEntity con archivo CSV
     */
    private ResponseEntity<byte[]> generarArchivoConsolidadoConServicio(
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta,
            String nombreQuery) {

        try {
            log.info("📊 Generando archivo consolidado usando ConsolidacionService");

            // ✅ PASO 1: Obtener datos consolidados del servicio especializado
            List<Map<String, Object>> datosConsolidados =
                    consolidacionService.consolidarDatos(
                            repositories,
                            nombreQuery,
                            consulta.getParametrosFiltros()
                    );

            log.info("✅ Datos consolidados obtenidos: {} registros", datosConsolidados.size());

            // ✅ PASO 2: Generar archivo CSV con los datos ya consolidados
            Path tempFile = Files.createTempFile("consolidado_", ".csv");

            try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {

                if (!datosConsolidados.isEmpty()) {
                    // Escribir encabezados
                    Set<String> headers = datosConsolidados.get(0).keySet();
                    writer.write(String.join(",", headers));
                    writer.newLine();

                    // Escribir datos
                    for (Map<String, Object> registro : datosConsolidados) {
                        writer.write(registro.values().stream()
                                .map(v -> v != null ? escaparCSV(v.toString()) : "")
                                .collect(Collectors.joining(",")));
                        writer.newLine();
                    }
                }

                log.info("✅ Archivo CSV generado: {} registros", datosConsolidados.size());
            }

            // Leer archivo y retornar response
            byte[] contenido = Files.readAllBytes(tempFile);
            Files.delete(tempFile);

            return construirRespuestaArchivo(
                    contenido,
                    generarNombreArchivoConsolidado("csv"),
                    "csv",
                    repositories.size()
            );

        } catch (Exception e) {
            log.error("❌ Error generando archivo consolidado: {}", e.getMessage(), e);
            throw new RuntimeException("Error generando archivo consolidado", e);
        }
    }

    /**
     * Genera archivo normal (sin consolidar) usando streaming.
     *
     * @param repositories Repositorios a consultar
     * @param consulta Parámetros
     * @param nombreQuery Nombre de la query
     * @param formato Formato de salida (json, csv, excel)
     * @return ResponseEntity con archivo generado
     */
    private ResponseEntity<byte[]> generarArchivoNormal(
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta,
            String nombreQuery,
            String formato) {

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            // Inicializar contexto de streaming según formato
            context = streamingConverter.inicializarStreaming(formato, outputStream);
            if (context == null) {
                throw new RuntimeException("No se pudo inicializar streaming para " + formato);
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            // Procesar en lotes y escribir directamente al stream
            batchProcessor.procesarEnLotes(
                    repositories,
                    consulta.getParametrosFiltros(),
                    nombreQuery,
                    lote -> procesarLoteParaArchivo(finalContext, lote)
            );

            // Finalizar y cerrar archivo
            streamingConverter.finalizarStreaming(context);

            return construirRespuestaArchivo(
                    outputStream.toByteArray(),
                    generarNombreArchivo(formato),
                    formato,
                    repositories.size()
            );

        } catch (Exception e) {
            log.error("❌ Error generando archivo: {}", e.getMessage(), e);
            limpiarContextoStreaming(context);
            throw new RuntimeException("Error generando archivo", e);
        }
    }

    /**
     * Procesa un lote de datos para escritura en archivo.
     *
     * @param context Contexto de streaming
     * @param lote Datos a escribir
     */
    private void procesarLoteParaArchivo(
            StreamingFormatoConverter.StreamingContext context,
            List<Map<String, Object>> lote) {
        try {
            if (lote != null && !lote.isEmpty()) {
                streamingConverter.procesarLoteStreaming(context, lote);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error procesando lote para archivo", e);
        }
    }

    // =============== UTILIDADES ===============

    /**
     * Construye ResponseEntity con headers apropiados para descarga de archivo.
     *
     * @param data Contenido del archivo
     * @param filename Nombre del archivo
     * @param formato Formato del archivo
     * @param numProvincias Número de provincias consultadas
     * @return ResponseEntity configurado
     */
    private ResponseEntity<byte[]> construirRespuestaArchivo(
            byte[] data,
            String filename,
            String formato,
            int numProvincias) {

        HttpHeaders headers = new HttpHeaders();

        // Configurar descarga como attachment
        headers.setContentDisposition(
                ContentDisposition.builder("attachment")
                        .filename(filename, StandardCharsets.UTF_8)
                        .build()
        );

        // Determinar content type según formato
        headers.setContentType(determinarMediaType(formato));

        // Headers personalizados para metadata
        headers.set("X-Total-Provincias", String.valueOf(numProvincias));
        headers.set("X-Archivo-Tamano", String.valueOf(data.length));

        log.info("📁 Archivo generado: {} bytes, {} provincias, formato: {}",
                data.length, numProvincias, formato);

        return ResponseEntity.ok()
                .headers(headers)
                .body(data);
    }

    /**
     * Limpia recursos del contexto de streaming en caso de error.
     *
     * @param context Contexto a limpiar
     */
    private void limpiarContextoStreaming(StreamingFormatoConverter.StreamingContext context) {
        if (context != null) {
            try {
                streamingConverter.finalizarStreaming(context);
            } catch (Exception e) {
                log.warn("⚠️ Error limpiando contexto de streaming: {}", e.getMessage());
            }
        }
    }

    /**
     * Escapa caracteres especiales en valores CSV.
     *
     * CASOS ESPECIALES:
     * - Valores con comas → encerrar entre comillas
     * - Valores con comillas → duplicar comillas
     * - Valores con saltos de línea → encerrar entre comillas
     *
     * @param valor Valor a escapar
     * @return Valor escapado para CSV
     */
    private String escaparCSV(String valor) {
        if (valor.contains(",") || valor.contains("\"") || valor.contains("\n")) {
            return "\"" + valor.replace("\"", "\"\"") + "\"";
        }
        return valor;
    }

    /**
     * Genera nombre de archivo con timestamp para descargas normales.
     *
     * Formato: infracciones_YYYYMMDD_HHmmss.{formato}
     * Ejemplo: infracciones_20251015_143022.json
     *
     * @param formato Extensión del archivo
     * @return Nombre de archivo con timestamp
     */
    private String generarNombreArchivo(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_%s.%s", timestamp, formato.toLowerCase());
    }

    /**
     * Genera nombre de archivo con timestamp para descargas consolidadas.
     *
     * Formato: infracciones_consolidado_YYYYMMDD_HHmmss.{formato}
     * Ejemplo: infracciones_consolidado_20251015_143022.csv
     *
     * @param formato Extensión del archivo
     * @return Nombre de archivo consolidado con timestamp
     */
    private String generarNombreArchivoConsolidado(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_consolidado_%s.%s", timestamp, formato.toLowerCase());
    }

    /**
     * Determina el MediaType apropiado según el formato solicitado.
     *
     * FORMATOS SOPORTADOS:
     * - csv → text/csv
     * - excel/xlsx → application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
     * - json (default) → application/json
     *
     * @param formato Formato del archivo
     * @return MediaType correspondiente
     */
    private MediaType determinarMediaType(String formato) {
        switch (formato.toLowerCase()) {
            case "csv":
                return MediaType.parseMediaType("text/csv");
            case "excel":
            case "xlsx":
                return MediaType.parseMediaType(
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            case "json":
            default:
                return MediaType.APPLICATION_JSON;
        }
    }
}