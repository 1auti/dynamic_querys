package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.factory.RepositoryFactory;

import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Servicio principal para la gestión y ejecución de consultas de infracciones de tránsito.
 * Este servicio maneja:
 * - Ejecución de queries desde base de datos H2 (sistema moderno)
 * - Procesamiento síncrono y asíncrono por lotes
 * - Consolidación automática de datos multi-provincia
 * - Generación de archivos en múltiples formatos (JSON, CSV, Excel)
 * - Streaming de datos para consultas grandes
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

    @Autowired
    private ConsolidacionService consolidacionService;

    @Autowired
    private StreamingFormatoConverter streamingConverter;

    @Autowired
    private QueryStorageRepository queryStorageRepository;

    @Autowired
    private ParametrosProcessor parametrosProcessor;

    @Autowired
    private DynamicBuilderQuery builderQuery;

    // =============== CONFIGURACIÓN ===============

    @Value("${app.limits.max-records-sync:1000}")
    private int maxRecordsSincrono;

    @Value("${app.limits.max-records-total:50000}")
    private int maxRecordsTotal;

    @Value("${app.async.thread-pool-size:10}")
    private int threadPoolSize;

    private final Executor executor;

    // =============== CONSTRUCTOR Y LIFECYCLE ===============

    /**
     * Constructor que inicializa el pool de threads para procesamiento asíncrono.
     */
    public InfraccionesService() {
        this.executor = Executors.newFixedThreadPool(threadPoolSize > 0 ? threadPoolSize : 10);
        log.info("InfraccionesService inicializado con pool de {} threads", threadPoolSize);
    }

    /**
     * Limpia recursos al destruir el bean.
     */
    @PreDestroy
    public void cleanup() {
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).shutdown();
            log.info("Thread pool cerrado correctamente");
        }
    }

    // =============== API PÚBLICA PRINCIPAL ===============

    /**
     * Ejecuta una consulta por tipo desde base de datos y retorna resultado en formato JSON.
     *
     * @param tipoConsulta Código de la query almacenada en base de datos
     * @param consulta DTO con parámetros de la consulta y formato deseado
     * @return Object con los datos en formato JSON, puede ser consolidado automáticamente
     * @throws ValidationException si la query no existe en la base de datos
     * @throws IllegalArgumentException si el código de query es inválido
     * @throws RuntimeException si hay errores durante la ejecución
     */
    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("=== Iniciando ejecución de consulta: {} ===", tipoConsulta);

        validarTipoConsulta(tipoConsulta);
        return ejecutarQueryDesdeBaseDatos(tipoConsulta, consulta);
    }

    /**
     * Ejecuta una consulta por tipo y genera archivo descargable (CSV/Excel).
     *
     * @param tipoConsulta Código de la query almacenada en base de datos
     * @param consulta DTO con parámetros de la consulta y formato deseado
     * @return ResponseEntity<byte[]> con el archivo generado y headers HTTP apropiados
     * @throws ValidationException si la query no existe en la base de datos
     * @throws RuntimeException si hay errores durante la generación del archivo
     */
    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("=== Iniciando descarga de archivo: {} ===", tipoConsulta);

        validarTipoConsulta(tipoConsulta);
        return consultarInfraccionesComoArchivo(consulta, tipoConsulta);
    }

    /**
     * Determina qué repositorios usar basado en filtros, considerando automáticamente consolidación.
     *
     * @param filtros Parámetros de filtrado que pueden incluir provincias específicas
     * @return List<InfraccionesRepositoryImpl> Lista de repositorios a consultar
     */
    public List<InfraccionesRepositoryImpl> determinarRepositories(ParametrosFiltrosDTO filtros) {
        // Si consolidado está activo, usar TODAS las provincias disponibles
        if (filtros != null && filtros.esConsolidado()) {
            log.debug("Consolidación activa - usando todos los repositorios disponibles");
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }

        // Lógica normal para determinar repositorios
        if (filtros != null && filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()) {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        } else if (filtros != null && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            List<String> provinciasNormalizadas = validator.normalizarProvincias(filtros.getBaseDatos());

            return provinciasNormalizadas.stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(provincia -> (InfraccionesRepositoryImpl) repositoryFactory.getRepository(provincia))
                    .collect(Collectors.toList());
        } else {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Valida si una consulta puede ser consolidada.
     *
     * @param filtros Parámetros de filtrado a validar
     * @return boolean true si la consolidación es posible
     */
    public boolean puedeSerConsolidada(ParametrosFiltrosDTO filtros) {
        return consolidacionService.validarConsolidacion(filtros);
    }

    // =============== CORE: EJECUCIÓN DESDE BASE DE DATOS ===============

    /**
     * Método principal que ejecuta queries desde base de datos H2 con soporte completo
     * de consolidación automática y procesamiento optimizado.
     *
     * @param codigoQuery Código único de la query en base de datos
     * @param consulta DTO con parámetros y configuración
     * @return Object Resultado procesado, puede ser JSON simple o consolidado
     * @throws ValidationException si los parámetros no son válidos
     * @throws IllegalArgumentException si la query no existe
     * @throws IllegalStateException si la query no está disponible
     * @throws RuntimeException si hay errores durante la ejecución
     */
    public Object ejecutarQueryDesdeBaseDatos(String codigoQuery, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("Ejecutando query desde BD: {} - Consolidado: {}",
                codigoQuery, consulta.getParametrosFiltros() != null &&
                        consulta.getParametrosFiltros().esConsolidado());

        // 1. Obtener y validar query de la base de datos
        QueryStorage queryStorage = obtenerYValidarQuery(codigoQuery);

        // 2. Registrar uso para estadísticas
        registrarUsoQuery(queryStorage);

        // 3. Validar parámetros de consulta
        validator.validarConsulta(consulta);

        // 4. Determinar repositorios necesarios
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No se encontraron repositorios válidos para la query: {}", codigoQuery);
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // 5. Ejecutar según tipo de procesamiento
        return ejecutarSegunTipoProcesamiento(queryStorage, repositories, consulta);
    }

    /**
     * Genera archivo descargable procesando la consulta con streaming optimizado.
     *
     * @param consulta DTO con parámetros de la consulta
     * @param nombreQuery Código de la query para identificar el tipo
     * @return ResponseEntity<byte[]> Archivo con headers HTTP apropiados
     * @throws ValidationException si los parámetros no son válidos
     * @throws RuntimeException si hay errores durante la generación
     */
    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(ConsultaQueryDTO consulta, String nombreQuery)
            throws ValidationException {

        log.info("Generando archivo para consulta con query: {}", nombreQuery);
        validator.validarConsulta(consulta);

        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        // Decisión automática: archivo consolidado vs normal
        if (consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado()) {
            log.info("Generando archivo consolidado para {} provincias", repositories.size());
            return generarArchivoConsolidado(repositories, consulta, nombreQuery, formato);
        }

        log.info("Generando archivo normal para {} repositorios", repositories.size());
        return generarArchivoNormal(repositories, consulta, nombreQuery, formato);
    }

    // =============== MÉTODOS PRIVADOS: VALIDACIÓN Y OBTENCIÓN ===============

    /**
     * Valida que el tipo de consulta exista en la base de datos.
     *
     * @param tipoConsulta Código de la consulta a validar
     * @throws ValidationException si no hay queries en la base o si el código no existe
     */
    private void validarTipoConsulta(String tipoConsulta) throws ValidationException {
        List<String> nombresQuerys = queryStorageRepository.findAllbyCodigo();

        if (nombresQuerys.isEmpty()) {
            throw new ValidationException("No hay queries disponibles en la base de datos");
        }

        if (!nombresQuerys.contains(tipoConsulta)) {
            throw new ValidationException("La query '" + tipoConsulta + "' no existe. " +
                    "Queries disponibles: " + nombresQuerys);
        }
    }

    /**
     * Obtiene y valida una query desde la base de datos.
     *
     * @param codigoQuery Código único de la query
     * @return QueryStorage Objeto con la query y metadata
     * @throws IllegalArgumentException si la query no existe
     * @throws IllegalStateException si la query no está disponible
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
     * Registra el uso de una query para estadísticas.
     *
     * @param queryStorage Query a registrar
     */
    private void registrarUsoQuery(QueryStorage queryStorage) {
        try {
            queryStorage.registrarUso();
            queryStorageRepository.save(queryStorage);
            log.debug("Uso registrado para query: {}", queryStorage.getCodigo());
        } catch (Exception e) {
            log.warn("Error registrando uso de query {}: {}", queryStorage.getCodigo(), e.getMessage());
        }
    }

    /**
     * Ejecuta la query según el tipo de procesamiento requerido.
     *
     * @param queryStorage Metadata de la query
     * @param repositories Lista de repositorios a consultar
     * @param consulta Parámetros de la consulta
     * @return Object Resultado procesado
     * @throws ValidationException si hay errores de validación
     */
    private Object ejecutarSegunTipoProcesamiento(QueryStorage queryStorage,
                                                  List<InfraccionesRepositoryImpl> repositories,
                                                  ConsultaQueryDTO consulta) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Verificar si requiere consolidación
        boolean esConsolidado = filtros != null &&
                filtros.esConsolidado() &&
                queryStorage.getEsConsolidable();

        // CORREGIDO: Lógica mejorada para detectar cuándo usar lotes
        boolean usarLotes = false;
        if (filtros != null) {
            // Usar lotes si:
            boolean todasLasBDS = Boolean.TRUE.equals(filtros.getUsarTodasLasBDS());
            int limiteEfectivo = filtros.getLimiteEfectivo();
            int numRepositorios = repositories.size();

            usarLotes = todasLasBDS ||                          // Todas las BDS
                    limiteEfectivo > 25000 ||                // Límite alto
                    numRepositorios >= 4 ||                  // Muchas provincias
                    limiteEfectivo == Integer.MAX_VALUE;     // Sin límite

            log.info("Análisis para lotes - TodasBDS: {}, Límite: {}, Repos: {}, Usar lotes: {}",
                    todasLasBDS, limiteEfectivo, numRepositorios, usarLotes);
        }

        if (esConsolidado) {
            log.info("🔄 Modo CONSOLIDADO para query BD: {}", queryStorage.getCodigo());
            return ejecutarConsolidacionConQueryStorage(queryStorage, repositories, consulta);
        }
        else if (usarLotes) {
            log.info("📦 Modo LOTES para query: {} (repos: {}, límite: {})",
                    queryStorage.getCodigo(), repositories.size(),
                    filtros != null ? filtros.getLimiteEfectivo() : "null");
            return ejecutarConLotes(queryStorage, repositories, consulta);
        }
        else {
            log.info("⚡ Modo NORMAL para query: {}", queryStorage.getCodigo());
            return ejecutarQueryStorageNormal(queryStorage, repositories, consulta);
        }
    }

    // =============== MÉTODOS PRIVADOS: CONSOLIDACIÓN ===============

    private Object ejecutarConLotes(QueryStorage queryStorage,
                                    List<InfraccionesRepositoryImpl> repositories,
                                    ConsultaQueryDTO consulta){
        List<Map<String,Object>> resultados = new ArrayList<>();

        batchProcessor.procesarEnLotes(
                repositories,
                consulta.getParametrosFiltros(),
                queryStorage.getCodigo(),
                lote -> resultados.addAll(lote)
        );

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultados,formato);

    }

    /**
     * Ejecuta consolidación usando metadata de QueryStorage.
     *
     * @param queryStorage Query con metadata de consolidación
     * @param repositories Repositorios de provincias
     * @param consulta Parámetros de consulta
     * @return Object Datos consolidados
     * @throws ValidationException si hay errores de validación
     * @throws RuntimeException si hay errores durante la consolidación
     */
    private Object ejecutarConsolidacionConQueryStorage(QueryStorage queryStorage,
                                                        List<InfraccionesRepositoryImpl> repositories,
                                                        ConsultaQueryDTO consulta) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        try {
            // Recopilar datos de todas las provincias usando SQL de BD
            List<Map<String, Object>> todosLosDatos = recopilarDatosConQueryStorage(queryStorage, repositories, filtros);

            if (todosLosDatos.isEmpty()) {
                return formatoConverter.convertir(Collections.emptyList(),
                        consulta.getFormato() != null ? consulta.getFormato() : "json");
            }

            // Normalizar provincias y consolidar
            todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);
            List<Map<String, Object>> datosConsolidados = consolidarConMetadataDeBaseDatos(
                    todosLosDatos, filtros, queryStorage);

            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return formatoConverter.convertir(datosConsolidados, formato);

        } catch (Exception e) {
            log.error("Error en consolidación con QueryStorage '{}': {}", queryStorage.getCodigo(), e.getMessage(), e);
            throw new RuntimeException("Error ejecutando consolidación desde BD", e);
        }
    }

    /**
     * Recopila datos de todas las provincias usando el SQL almacenado en QueryStorage.
     *
     * @param queryStorage Query con SQL a ejecutar
     * @param repositories Lista de repositorios de provincias
     * @param filtros Parámetros de filtrado
     * @return List<Map<String, Object>> Datos recopilados de todas las provincias
     */
    private List<Map<String, Object>> recopilarDatosConQueryStorage(QueryStorage queryStorage,
                                                                    List<InfraccionesRepositoryImpl> repositories,
                                                                    ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {

                String sqlDinamico = queryStorage.getSqlQuery();

                // 2. Procesar parámetros con el SQL de BD
                ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                        sqlDinamico, filtros);

                // 3. Ejecutar query SQL de BD
                List<Map<String, Object>> datosProvider = repo.getNamedParameterJdbcTemplate().queryForList(
                        resultado.getQueryModificada(),
                        resultado.getParametros()
                );

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar metadata de provincia
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                        registro.put("query_codigo", queryStorage.getCodigo());
                    }

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Query BD '{}' en provincia {}: {} registros",
                            queryStorage.getCodigo(), provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error ejecutando QueryStorage '{}' en provincia {}: {}",
                        queryStorage.getCodigo(), provincia, e.getMessage());
            }
        }

        log.info("QueryStorage '{}': Total recopilado {} registros de {} provincias",
                queryStorage.getCodigo(), todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    /**
     * Consolida datos usando metadata de la base de datos.
     *
     * @param datos Datos a consolidar
     * @param filtros Parámetros de filtrado (incluye preferencias del usuario)
     * @param queryStorage Query con metadata de consolidación
     * @return List<Map<String, Object>> Datos consolidados
     */
    private List<Map<String, Object>> consolidarConMetadataDeBaseDatos(List<Map<String, Object>> datos,
                                                                       ParametrosFiltrosDTO filtros,
                                                                       QueryStorage queryStorage) {

        if (datos.isEmpty()) {
            return Collections.emptyList();
        }

        // Usar metadata de consolidación de BD
        List<String> camposAgrupacion = determinarCamposAgrupacionDesdeBD(filtros, queryStorage);
        List<String> camposNumericos = queryStorage.getCamposNumericosList();

        log.info("Consolidación desde BD - Query: {}, Agrupación: {}, Numéricos: {}",
                queryStorage.getCodigo(), camposAgrupacion, camposNumericos);

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Determina campos de agrupación combinando preferencias del usuario con metadata de BD.
     *
     * @param filtros Filtros con preferencias del usuario
     * @param queryStorage Query con campos válidos
     * @return List<String> Campos de agrupación finales
     */
    private List<String> determinarCamposAgrupacionDesdeBD(ParametrosFiltrosDTO filtros, QueryStorage queryStorage) {

        List<String> camposAgrupacion = new ArrayList<>();
        List<String> consolidacionUsuario = filtros.getConsolidacionSeguro();

        if (!consolidacionUsuario.isEmpty()) {
            // Usuario especificó campos - validar contra BD
            List<String> camposValidosBD = queryStorage.getCamposAgrupacionList();

            for (String campo : consolidacionUsuario) {
                if (camposValidosBD.contains(campo)) {
                    camposAgrupacion.add(campo);
                    log.debug("Campo '{}' válido desde BD para query '{}'", campo, queryStorage.getCodigo());
                } else {
                    log.warn("Campo '{}' NO válido según BD para query '{}'", campo, queryStorage.getCodigo());
                }
            }
        }

        // Si no hay campos válidos del usuario, usar los de BD
        if (camposAgrupacion.isEmpty()) {
            camposAgrupacion.addAll(queryStorage.getCamposAgrupacionList());
            log.info("Usando campos de agrupación de BD para query '{}': {}",
                    queryStorage.getCodigo(), camposAgrupacion);
        }

        // Garantizar que siempre haya "provincia"
        if (!camposAgrupacion.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        return camposAgrupacion.stream().distinct().collect(Collectors.toList());
    }

    // =============== MÉTODOS PRIVADOS: PROCESAMIENTO NORMAL ===============

    /**
     * Ejecuta QueryStorage en modo normal (sin consolidación) usando paralelismo.
     *
     * @param queryStorage Query a ejecutar
     * @param repositories Repositorios de provincias
     * @param consulta Parámetros de consulta
     * @return Object Datos en formato solicitado
     * @throws ValidationException si hay errores de validación
     */
    private Object ejecutarQueryStorageNormal(QueryStorage queryStorage,
                                              List<InfraccionesRepositoryImpl> repositories,
                                              ConsultaQueryDTO consulta) throws ValidationException {

        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();

                        // 1. Construir SQL Dinamico
                        String sqlDinamico = queryStorage.getSqlQuery();

                        // 2. Procesar parámetros con SQL de BD
                        ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                                sqlDinamico, consulta.getParametrosFiltros());

                        // 3. Ejecutar query
                        List<Map<String, Object>> datos = repo.getNamedParameterJdbcTemplate().queryForList(
                                resultado.getQueryModificada(),
                                resultado.getParametros()
                        );

                        // Agregar metadata a cada registro
                        datos.forEach(registro -> {
                            registro.put("provincia", provincia);
                            registro.put("query_codigo", queryStorage.getCodigo());
                        });

                        return datos.stream();

                    } catch (Exception e) {
                        log.error("Error ejecutando QueryStorage '{}' en provincia {}: {}",
                                queryStorage.getCodigo(), repo.getProvincia(), e.getMessage());
                        return java.util.stream.Stream.empty();
                    }
                })
                .collect(Collectors.toList());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    // =============== MÉTODOS PRIVADOS: GENERACIÓN DE ARCHIVOS ===============

    /**
     * Genera archivo consolidado procesando cada provincia secuencialmente.
     *
     * @param repositories Lista de repositorios
     * @param consulta Parámetros de consulta
     * @param nombreQuery Nombre de la query
     * @param formato Formato del archivo (csv, excel, json)
     * @return ResponseEntity<byte[]> Archivo con headers HTTP
     * @throws RuntimeException si hay errores durante la generación
     */
    private ResponseEntity<byte[]> generarArchivoConsolidado(List<InfraccionesRepositoryImpl> repositories,
                                                             ConsultaQueryDTO consulta, String nombreQuery, String formato) {

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar streaming para archivo consolidado");
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            // Procesar cada provincia secuencialmente
            for (InfraccionesRepositoryImpl repo : repositories) {
                procesarProvinciaParaArchivo(repo, consulta, nombreQuery, finalContext, true);
            }

            streamingConverter.finalizarStreaming(context);

            return construirRespuestaArchivo(outputStream.toByteArray(),
                    generarNombreArchivoConsolidado(formato), formato, repositories.size());

        } catch (Exception e) {
            log.error("Error generando archivo consolidado: {}", e.getMessage(), e);
            limpiarContextoStreaming(context);
            throw new RuntimeException("Error generando archivo consolidado", e);
        }
    }

    /**
     * Genera archivo normal usando procesamiento por lotes.
     *
     * @param repositories Lista de repositorios
     * @param consulta Parámetros de consulta
     * @param nombreQuery Nombre de la query
     * @param formato Formato del archivo
     * @return ResponseEntity<byte[]> Archivo con headers HTTP
     * @throws RuntimeException si hay errores durante la generación
     */
    private ResponseEntity<byte[]> generarArchivoNormal(List<InfraccionesRepositoryImpl> repositories,
                                                        ConsultaQueryDTO consulta, String nombreQuery, String formato) {

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming para archivo");
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            batchProcessor.procesarEnLotes(
                    repositories,
                    consulta.getParametrosFiltros(),
                    nombreQuery,
                    lote -> {
                        try {
                            if (lote != null && !lote.isEmpty()) {
                                streamingConverter.procesarLoteStreaming(finalContext, lote);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error procesando lote para archivo", e);
                        }
                    }
            );

            streamingConverter.finalizarStreaming(context);

            return construirRespuestaArchivo(outputStream.toByteArray(),
                    generarNombreArchivo(formato), formato, repositories.size());

        } catch (Exception e) {
            log.error("Error generando archivo: {}", e.getMessage(), e);
            limpiarContextoStreaming(context);
            throw new RuntimeException("Error generando archivo", e);
        }
    }

    /**
     * Procesa una provincia específica para generación de archivo.
     *
     * @param repo Repository de la provincia
     * @param consulta Parámetros de consulta
     * @param nombreQuery Nombre de la query
     * @param context Contexto de streaming
     * @param esConsolidado Si es procesamiento consolidado
     */
    private void procesarProvinciaParaArchivo(InfraccionesRepositoryImpl repo,
                                              ConsultaQueryDTO consulta,
                                              String nombreQuery,
                                              StreamingFormatoConverter.StreamingContext context,
                                              boolean esConsolidado) {
        String provincia = repo.getProvincia();

        try {
            log.debug("Procesando provincia para archivo {}: {}",
                    esConsolidado ? "consolidado" : "normal", provincia);

            List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(
                    nombreQuery, consulta.getParametrosFiltros());

            if (datosProvider != null && !datosProvider.isEmpty()) {
                // Agregar información de provincia
                datosProvider.forEach(registro -> {
                    registro.put("provincia", provincia);
                    if (esConsolidado) {
                        registro.put("fuente_consolidacion", true);
                    }
                });

                // Procesar en streaming
                streamingConverter.procesarLoteStreaming(context, datosProvider);
            }

        } catch (Exception e) {
            log.error("Error procesando provincia {} para archivo: {}", provincia, e.getMessage(), e);

            // Agregar registro de error en lugar de fallar completamente
            Map<String, Object> registroError = new HashMap<>();
            registroError.put("provincia", provincia);
            registroError.put("error_consolidacion", true);
            registroError.put("mensaje_error", e.getMessage());
            registroError.put("timestamp_error", new Date());

            try {
                streamingConverter.procesarLoteStreaming(context, Collections.singletonList(registroError));
            } catch (Exception errorException) {
                log.error("Error agregando registro de error para provincia {}: {}",
                        provincia, errorException.getMessage());
            }
        }
    }

    // =============== MÉTODOS UTILITARIOS PRIVADOS ===============

    /**
     * Construye la respuesta HTTP para archivos descargables.
     *
     * @param data Datos del archivo
     * @param filename Nombre del archivo
     * @param formato Formato del archivo
     * @param numProvincias Número de provincias procesadas
     * @return ResponseEntity<byte[]> Respuesta HTTP completa
     */
    private ResponseEntity<byte[]> construirRespuestaArchivo(byte[] data, String filename,
                                                             String formato, int numProvincias) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentDispositionFormData("attachment", filename);
        headers.setContentType(determinarMediaType(formato));

        // Headers adicionales para información
        headers.set("X-Total-Provincias", String.valueOf(numProvincias));
        headers.set("X-Archivo-Tamaño", String.valueOf(data.length));

        log.info("Archivo generado exitosamente: {} bytes, {} provincias, formato: {}",
                data.length, numProvincias, formato);

        return ResponseEntity.ok().headers(headers).body(data);
    }

    /**
     * Limpia contexto de streaming en caso de error.
     *
     * @param context Contexto a limpiar
     */
    private void limpiarContextoStreaming(StreamingFormatoConverter.StreamingContext context) {
        if (context != null) {
            try {
                streamingConverter.finalizarStreaming(context);
            } catch (Exception cleanupException) {
                log.warn("Error limpiando contexto de streaming: {}", cleanupException.getMessage());
            }
        }
    }

    /**
     * Normaliza provincias en los datos para consistencia.
     *
     * @param datos Datos a normalizar
     * @return List<Map<String, Object>> Datos con provincias normalizadas
     */
    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = org.transito_seguro.utils.NormalizadorProvincias.normalizar(provincia);

            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        return datos;
    }

    /**
     * Obtiene la provincia de un registro buscando en múltiples campos.
     *
     * @param registro Registro a procesar
     * @return String Nombre de la provincia encontrada
     */
    private String obtenerProvinciaDelRegistro(Map<String, Object> registro) {
        String[] camposProvincia = {"provincia", "provincia_origen", "contexto"};

        for (String campo : camposProvincia) {
            Object valor = registro.get(campo);
            if (valor != null && !valor.toString().trim().isEmpty()) {
                return valor.toString().trim();
            }
        }

        return "SIN_PROVINCIA";
    }

    /**
     * Consolida datos por campos específicos usando agrupación y suma.
     *
     * @param datos Datos a consolidar
     * @param camposAgrupacion Campos para agrupar
     * @param camposNumericos Campos numéricos a sumar
     * @return List<Map<String, Object>> Datos consolidados
     */
    private List<Map<String, Object>> consolidarPorCampos(List<Map<String, Object>> datos,
                                                          List<String> camposAgrupacion,
                                                          List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("No hay campos de agrupación, retornando datos sin consolidar");
            return datos;
        }

        log.info("Consolidando {} registros por campos: {}", datos.size(), camposAgrupacion);

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoNuevo(registro, camposAgrupacion, camposNumericos));

            sumarCamposNumericos(grupo, registro, camposNumericos);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());
        log.info("Consolidación completada: {} registros → {} grupos", datos.size(), resultado.size());

        return resultado;
    }

    /**
     * Crea un grupo nuevo para consolidación copiando campos de agrupación.
     *
     * @param registro Registro base
     * @param camposAgrupacion Campos a copiar
     * @param camposNumericos Campos numéricos a inicializar en 0
     * @return Map<String, Object> Nuevo grupo inicializado
     */
    private Map<String, Object> crearGrupoNuevo(Map<String, Object> registro,
                                                List<String> camposAgrupacion,
                                                List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Copiar campos de agrupación
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Inicializar campos numéricos en 0
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    /**
     * Suma campos numéricos de un registro a un grupo existente.
     *
     * @param grupo Grupo acumulador
     * @param registro Registro con valores a sumar
     * @param camposNumericos Campos numéricos a procesar
     */
    private void sumarCamposNumericos(Map<String, Object> grupo,
                                      Map<String, Object> registro,
                                      List<String> camposNumericos) {
        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);

            if (valorRegistro == null) continue;

            Long valorNumerico = convertirANumero(valorRegistro);
            if (valorNumerico == null) continue;

            Long valorActual = obtenerValorNumerico(grupo.get(campo));
            grupo.put(campo, valorActual + valorNumerico);
        }
    }

    /**
     * Crea clave única de grupo concatenando valores de campos de agrupación.
     *
     * @param registro Registro a procesar
     * @param camposAgrupacion Campos para crear la clave
     * @return String Clave única del grupo
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * Convierte un valor a número Long manejando diferentes tipos.
     *
     * @param valor Valor a convertir
     * @return Long Número convertido o null si no es posible
     */
    private Long convertirANumero(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }

        if (valor instanceof String) {
            String str = valor.toString().trim();
            if (!str.isEmpty()) {
                try {
                    return Math.round(Double.parseDouble(str));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }

        return null;
    }

    /**
     * Obtiene valor numérico existente, retorna 0 si no es numérico.
     *
     * @param valor Valor a procesar
     * @return Long Valor numérico o 0
     */
    private Long obtenerValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

    /**
     * Genera nombre único para archivo usando timestamp.
     *
     * @param formato Extensión del archivo
     * @return String Nombre del archivo con timestamp
     */
    private String generarNombreArchivo(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_%s.%s", timestamp, formato.toLowerCase());
    }

    /**
     * Genera nombre único para archivo consolidado usando timestamp.
     *
     * @param formato Extensión del archivo
     * @return String Nombre del archivo consolidado con timestamp
     */
    private String generarNombreArchivoConsolidado(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_consolidado_%s.%s", timestamp, formato.toLowerCase());
    }

    /**
     * Determina el MediaType apropiado según el formato de archivo.
     *
     * @param formato Formato del archivo (csv, excel, json)
     * @return MediaType Tipo MIME correspondiente
     */
    private MediaType determinarMediaType(String formato) {
        switch (formato.toLowerCase()) {
            case "csv":
                return MediaType.parseMediaType("text/csv");
            case "excel":
                return MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            case "json":
            default:
                return MediaType.APPLICATION_JSON;
        }
    }

    // =============== MÉTODOS LEGACY - DEPRECADOS ===============

    /**
     * @deprecated Usar {@link #ejecutarQueryDesdeBaseDatos(String, ConsultaQueryDTO)}
     * Método legacy mantenido solo para compatibilidad temporal.
     *
     * @param consulta DTO con parámetros
     * @param nombreQuery Nombre de la query (archivo)
     * @return Object Resultado procesado
     * @throws ValidationException si hay errores de validación
     */
    @Deprecated
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        log.warn("Método DEPRECADO consultarInfracciones() usado. Migrar a ejecutarQueryDesdeBaseDatos()");

        // Por ahora, redirigir al método moderno si la query existe en BD
        try {
            return ejecutarQueryDesdeBaseDatos(nombreQuery, consulta);
        } catch (IllegalArgumentException e) {
            log.warn("Query '{}' no encontrada en BD, usando método legacy", nombreQuery);
            throw new RuntimeException("Método legacy no implementado. Usar queries de base de datos.");
        }
    }
}