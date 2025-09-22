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
import org.transito_seguro.enums.Consultas;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

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

@Slf4j
@Service
public class InfraccionesService {

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

    @Value("${app.limits.max-records-sync:1000}")
    private int maxRecordsSincrono;

    @Value("${app.limits.max-records-total:50000}")
    private int maxRecordsTotal;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryMB;

    @Value("${app.async.thread-pool-size:10}")
    private int threadPoolSize;

    private final Executor executor;

    /**
     * Mapeo centralizado de tipos de consulta a archivos SQL
     */
    private static final Map<String, String> QUERY_MAPPING = Consultas.getMapeoCompleto();

    // Constructor para inicializar el thread pool
    public InfraccionesService() {
        this.executor = Executors.newFixedThreadPool(threadPoolSize > 0 ? threadPoolSize : 10);
    }

    @PreDestroy
    public void cleanup() {
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).shutdown();
        }
    }

    // =============== MÉTODOS PÚBLICOS PRINCIPALES ===============

    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfracciones(consulta, nombreQuery);
    }

    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfraccionesComoArchivo(consulta, nombreQuery);
    }

    /**
     * Método principal que maneja AUTOMÁTICAMENTE consolidación y procesamiento normal
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        log.info("Iniciando consulta de infracciones con query: {}", nombreQuery);

        // 1. Validar consulta
        validator.validarConsulta(consulta);

        // 2. Determinar repositorios (considera consolidación automáticamente)
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No se encontraron repositorios válidos para la consulta");
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // 3. LÓGICA INTEGRADA: Verificar si es consolidación
        if (consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado()) {
            log.info("Modo consolidado detectado - ejecutando consolidación para {} provincias", repositories.size());
            return ejecutarConsolidacion(repositories, consulta, nombreQuery);
        }

        // 4. Procesamiento normal (no consolidado)
        log.info("Modo normal - procesando {} repositorios", repositories.size());

        // Estimar tamaño de resultado
        int estimacionRegistros = estimarTamanoResultado(repositories, consulta.getParametrosFiltros(), nombreQuery);
        log.info("Estimación de registros: {}", estimacionRegistros);

        // Decidir estrategia de procesamiento
        if (estimacionRegistros <= maxRecordsSincrono) {
            log.info("Usando procesamiento síncrono (estimación: {} registros)", estimacionRegistros);
            return procesarSincrono(repositories, consulta, nombreQuery);
        } else {
            log.info("Usando procesamiento por lotes (estimación: {} registros)", estimacionRegistros);
            return procesarEnLotes(repositories, consulta, nombreQuery);
        }
    }

    /**
     * Método que maneja archivos - TAMBIÉN integra consolidación automáticamente
     */
    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(ConsultaQueryDTO consulta, String nombreQuery)
            throws ValidationException {

        log.info("Generando archivo para consulta con query: {}", nombreQuery);

        validator.validarConsulta(consulta);

        // Determinar repositorios (considera consolidación automáticamente)
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        // LÓGICA INTEGRADA: Verificar si es consolidación para archivos
        if (consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado()) {
            log.info("Generando archivo consolidado para {} provincias", repositories.size());
            return generarArchivoConsolidado(repositories, consulta, nombreQuery, formato);
        }

        // Procesamiento normal de archivos
        return generarArchivoNormal(repositories, consulta, nombreQuery, formato);
    }

    public Object ejecutarConsultaPorTipoMejorado(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {

        // 1. Intentar desde base de datos primero
        try {
            String codigoQuery = convertirTipoACodigoBD(tipoConsulta);
            Optional<QueryStorage> queryStorage = queryStorageRepository.findByCodigo(codigoQuery);

            if (queryStorage.isPresent() && queryStorage.get().estaLista()) {
                log.info("Ejecutando desde BD: {} -> {}", tipoConsulta, codigoQuery);
                return ejecutarQueryDesdeBaseDatos(codigoQuery, consulta);
            }
        } catch (Exception e) {
            log.debug("No disponible en BD, usando archivo para: {}", tipoConsulta);
        }

        // 2. Fallback al método original (archivos)
        return ejecutarConsultaPorTipo(tipoConsulta, consulta);
    }

    public Object ejecutarQueryDesdeBaseDatos(String codigoQuery, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("Ejecutando query desde BD: {} - Consolidado: {}",
                codigoQuery, consulta.getParametrosFiltros() != null &&
                        consulta.getParametrosFiltros().esConsolidado());

        // 1. Obtener query de la base de datos
        Optional<QueryStorage> queryStorageOpt = queryStorageRepository.findByCodigo(codigoQuery);
        if (!queryStorageOpt.isPresent()) {
            throw new IllegalArgumentException("Query no encontrada: " + codigoQuery);
        }

        QueryStorage queryStorage = queryStorageOpt.get();
        if (!queryStorage.estaLista()) {
            throw new IllegalStateException("Query no disponible: " + codigoQuery);
        }

        // 2. Registrar uso
        queryStorage.registrarUso();
        queryStorageRepository.save(queryStorage);

        // 3. Validar consulta (usando tu validador existente)
        validator.validarConsulta(consulta);

        // 4. Determinar repositorios (reutilizar lógica existente)
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No se encontraron repositorios válidos para la query: {}", codigoQuery);
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // 5. Ejecutar según consolidación (usando sistema existente)
        if (consulta.getParametrosFiltros() != null &&
                consulta.getParametrosFiltros().esConsolidado() &&
                queryStorage.getEsConsolidable()) {

            log.info("Modo consolidado detectado para query BD: {}", codigoQuery);
            return ejecutarConsolidacionConQueryStorage(queryStorage, repositories, consulta);
        }

        // 6. Procesamiento normal
        log.info("Modo normal para query BD: {}", codigoQuery);
        return ejecutarQueryStorageNormal(queryStorage, repositories, consulta);
    }

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

            // Normalizar provincias (reutilizar lógica existente)
            todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

            // Consolidar usando metadata de BD
            List<Map<String, Object>> datosConsolidados = consolidarConMetadataDeBaseDatos(
                    todosLosDatos, filtros, queryStorage);

            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return formatoConverter.convertir(datosConsolidados, formato);

        } catch (Exception e) {
            log.error("Error en consolidación con QueryStorage '{}': {}", queryStorage.getCodigo(), e.getMessage(), e);
            throw new RuntimeException("Error ejecutando consolidación desde BD", e);
        }
    }

    private List<Map<String, Object>> recopilarDatosConQueryStorage(QueryStorage queryStorage,
                                                                    List<InfraccionesRepositoryImpl> repositories,
                                                                    ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                // Procesar parámetros con el SQL de BD
                ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                        queryStorage.getSqlQuery(), filtros);

                // Ejecutar query SQL de BD
                List<Map<String, Object>> datosProvider = repo.getJdbcTemplate().queryForList(
                        resultado.getQueryModificada(),
                        resultado.getParametros()
                );

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar metadata de provincia
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                        registro.put("query_codigo", queryStorage.getCodigo()); // Para auditoría
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

    // =============== MÉTODOS PRIVADOS - CONSOLIDACIÓN ===============

    /**
     * Ejecuta consolidación (llamado automáticamente cuando consolidado=true)
     */

    private Object ejecutarConsolidacion(List<InfraccionesRepositoryImpl> repositories,
                                         ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Validar que la consolidación es posible
        if (!consolidacionService.validarConsolidacion(filtros)) {
            throw new ValidationException("Consolidación no disponible con los filtros especificados");
        }

        try {
            // Ejecutar consolidación
            List<Map<String, Object>> datosConsolidados = consolidacionService.consolidarDatos(
                    repositories, nombreQuery, filtros);

            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

            // ✨ USAR EL NUEVO MÉTODO OPTIMIZADO PARA GENERAR LA RESPUESTA
            Object resultadoOptimo = consolidacionService.generarRespuestaConsolidadaOptima(
                    datosConsolidados, formato);

            // Si no es formato optimizado, usar el convertidor tradicional
            if (resultadoOptimo == datosConsolidados) {
                return formatoConverter.convertir(datosConsolidados, formato);
            }

            // Para formato JSON optimizado, convertir a JSON string si es necesario
            if ("json".equals(formato) && resultadoOptimo instanceof Map) {
                try {
                    com.fasterxml.jackson.databind.ObjectMapper mapper =
                            new com.fasterxml.jackson.databind.ObjectMapper();
                    return mapper.writeValueAsString(resultadoOptimo);
                } catch (Exception e) {
                    log.warn("Error convirtiendo resultado optimizado a JSON: {}", e.getMessage());
                    return formatoConverter.convertir(datosConsolidados, formato);
                }
            }

            return resultadoOptimo;

        } catch (Exception e) {
            log.error("Error en consolidación: {}", e.getMessage(), e);
            throw new RuntimeException("Error ejecutando consolidación", e);
        }
    }

    /**
     * Genera archivo consolidado (llamado automáticamente cuando consolidado=true)
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
                String provincia = repo.getProvincia();

                try {
                    log.debug("Procesando provincia para archivo consolidado: {}", provincia);

                    List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(
                            nombreQuery, consulta.getParametrosFiltros());

                    if (datosProvider != null && !datosProvider.isEmpty()) {
                        // Agregar información de provincia
                        datosProvider.forEach(registro -> {
                            registro.put("provincia", provincia);
                            registro.put("fuente_consolidacion", true);
                        });

                        // Procesar en streaming
                        streamingConverter.procesarLoteStreaming(finalContext, datosProvider);
                    }

                } catch (Exception e) {
                    log.error("Error procesando provincia {} para archivo consolidado: {}",
                            provincia, e.getMessage(), e);

                    // Agregar registro de error
                    Map<String, Object> registroError = new HashMap<>();
                    registroError.put("provincia", provincia);
                    registroError.put("error_consolidacion", true);
                    registroError.put("mensaje_error", e.getMessage());
                    registroError.put("timestamp_error", new java.util.Date());

                    streamingConverter.procesarLoteStreaming(finalContext,
                            Collections.singletonList(registroError));
                }
            }

            streamingConverter.finalizarStreaming(context);

            // Preparar respuesta HTTP
            HttpHeaders headers = new HttpHeaders();
            String filename = generarNombreArchivoConsolidado(formato);
            headers.setContentDispositionFormData("attachment", filename);
            headers.setContentType(determinarMediaType(formato));

            byte[] responseData = outputStream.toByteArray();
            log.info("Archivo consolidado generado: {} bytes, {} provincias",
                    responseData.length, repositories.size());

            return ResponseEntity.ok().headers(headers).body(responseData);

        } catch (Exception e) {
            log.error("Error generando archivo consolidado: {}", e.getMessage(), e);

            if (context != null) {
                try {
                    streamingConverter.finalizarStreaming(context);
                } catch (Exception cleanupException) {
                    log.warn("Error limpiando contexto consolidado: {}", cleanupException.getMessage());
                }
            }

            throw new RuntimeException("Error generando archivo consolidado", e);
        }
    }

    /**
     * Genera archivo normal (no consolidado)
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

            // Preparar headers HTTP
            HttpHeaders headers = new HttpHeaders();
            String filename = generarNombreArchivo(formato);
            headers.setContentDispositionFormData("attachment", filename);
            headers.setContentType(determinarMediaType(formato));

            byte[] responseData = outputStream.toByteArray();
            log.info("Archivo generado exitosamente: {} bytes", responseData.length);

            return ResponseEntity.ok().headers(headers).body(responseData);

        } catch (Exception e) {
            log.error("Error generando archivo: {}", e.getMessage(), e);

            if (context != null) {
                try {
                    streamingConverter.finalizarStreaming(context);
                } catch (Exception cleanupException) {
                    log.warn("Error limpiando contexto: {}", cleanupException.getMessage());
                }
            }

            throw new RuntimeException("Error generando archivo", e);
        }
    }

    // =============== MÉTODOS PRIVADOS - PROCESAMIENTO NORMAL ===============

    /**
     * Procesamiento síncrono tradicional (para consultas pequeñas)
     */
    private Object procesarSincrono(List<InfraccionesRepositoryImpl> repositories,
                                    ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();
                        List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(
                                nombreQuery, consulta.getParametrosFiltros()
                        );

                        // Agregar provincia a cada registro
                        resultado.forEach(registro -> registro.put("provincia", provincia));
                        return resultado.stream();

                    } catch (Exception e) {
                        log.error("Error en provincia {}: {}", repo.getProvincia(), e.getMessage());
                        return java.util.stream.Stream.empty();
                    }
                })
                .collect(Collectors.toList());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    /**
     * Procesamiento por lotes con streaming (para consultas grandes)
     */
    private Object procesarEnLotes(List<InfraccionesRepositoryImpl> repositories,
                                   ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        StreamingFormatoConverter.StreamingContext context = null;

        try {
            File tempFile = File.createTempFile("infracciones_", "." + formato);
            FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
            BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream, 8192);

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming");
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

                                if (finalContext.getTotalRegistros() % 5000 == 0) {
                                    batchProcessor.limpiarMemoria();
                                }
                            }
                        } catch (Exception e) {
                            log.error("Error procesando lote en streaming: {}", e.getMessage(), e);
                            throw new RuntimeException("Error en procesamiento por lotes", e);
                        }
                    }
            );

            streamingConverter.finalizarStreaming(context);

            outputStream.close();
            byte[] fileData = Files.readAllBytes(tempFile.toPath());
            tempFile.delete(); // Limpiar archivo temporal
            return new String(fileData, "UTF-8");

        } catch (Exception e) {
            log.error("Error en procesamiento por lotes: {}", e.getMessage(), e);

            if (context != null) {
                try {
                    streamingConverter.finalizarStreaming(context);
                } catch (Exception cleanupException) {
                    log.warn("Error cerrando contexto de streaming: {}", cleanupException.getMessage());
                }
            }

            throw new RuntimeException("Error procesando consulta por lotes", e);
        }
    }

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
     * NUEVO: Determinar campos de agrupación desde BD + usuario
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

    /**
     * NUEVO: Ejecutar QueryStorage en modo normal (sin consolidación)
     */
    private Object ejecutarQueryStorageNormal(QueryStorage queryStorage,
                                              List<InfraccionesRepositoryImpl> repositories,
                                              ConsultaQueryDTO consulta) throws ValidationException {

        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();

                        // Procesar parámetros con SQL de BD
                        ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                                queryStorage.getSqlQuery(), consulta.getParametrosFiltros());

                        // Ejecutar
                        List<Map<String, Object>> datos = repo.getJdbcTemplate().queryForList(
                                resultado.getQueryModificada(),
                                resultado.getParametros()
                        );

                        // Agregar provincia a cada registro
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

    /**
     * NUEVO: Convertir tipo de consulta tradicional a código de BD
     */
    private String convertirTipoACodigoBD(String tipoConsulta) {
        // Mapeo de tipos tradicionales a códigos de BD
        Map<String, String> mapeoTipos = new HashMap<>();
        mapeoTipos.put("personas-juridicas", "personas-juridicas");
        mapeoTipos.put("infracciones-general", "reporte-infracciones-general");
        mapeoTipos.put("infracciones-por-equipos", "reporte-infracciones-por-equipos");
        mapeoTipos.put("radar-fijo-por-equipo", "reporte-radar-fijo-por-equipo");
        mapeoTipos.put("semaforo-por-equipo", "reporte-semaforo-por-equipo");
        mapeoTipos.put("vehiculos-por-municipio", "reporte-vehiculos-por-municipio");
        mapeoTipos.put("sin-email-por-municipio", "reporte-sin-email-por-municipio");
        mapeoTipos.put("verificar-imagenes-radar", "verificar-imagenes-subidas-radar-concesion");
        mapeoTipos.put("infracciones-detallado", "reporte-infracciones-detallado");


        return mapeoTipos.getOrDefault(tipoConsulta, tipoConsulta);
    }

    /**
     * NUEVO: Método helper para normalizar provincias (reutilizar lógica existente)
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
     * NUEVO: Helper para obtener provincia de un registro
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
     * NUEVO: Consolidación por campos específicos - método optimizado
     */
    private List<Map<String, Object>> consolidarPorCampos(
            List<Map<String, Object>> datos,
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
        log.info("Consolidación completada: {} registros → {} grupos",
                datos.size(), resultado.size());

        return resultado;
    }

    /**
     * NUEVO: Crear grupo nuevo para consolidación
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
     * NUEVO: Sumar campos numéricos para consolidación
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
     * NUEVO: Crear clave de grupo para consolidación
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * NUEVO: Convertir valor a número
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
     * NUEVO: Obtener valor numérico existente
     */
    private Long obtenerValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

    // =============== MÉTODOS UTILITARIOS ===============

    /**
     * Determina repositorios considerando automáticamente consolidación
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
     * Agrega metadata de consolidación al resultado JSON
     */
    private Object agregarMetadataConsolidacion(Object resultado, Map<String, Object> resumen) {
        if (resultado instanceof String) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                Map<String, Object> resultadoMap = mapper.readValue((String) resultado, Map.class);

//                resultadoMap.put("consolidacion_resumen", resumen);
//                resultadoMap.put("es_consolidado", true);

                return mapper.writeValueAsString(resultadoMap);
            } catch (Exception e) {
                log.warn("No se pudo agregar metadata a resultado JSON: {}", e.getMessage());
                return resultado;
            }
        }
        return resultado;
    }

    private int estimarTamanoResultado(List<InfraccionesRepositoryImpl> repositories,
                                       ParametrosFiltrosDTO filtros, String nombreQuery) {
        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(10)
                .pagina(0)
                .build();

        int totalEstimado = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            try {
                List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);

                if (muestra.size() >= 10) {
                    totalEstimado += 1000;
                } else if (muestra.size() > 0) {
                    totalEstimado += muestra.size() * 10;
                }

            } catch (Exception e) {
                log.warn("No se pudo estimar para provincia {}: {}", repo.getProvincia(), e.getMessage());
                totalEstimado += 500;
            }
        }

        return Math.min(totalEstimado, maxRecordsTotal);
    }

    private String generarNombreArchivo(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_%s.%s", timestamp, formato.toLowerCase());
    }

    private String generarNombreArchivoConsolidado(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_consolidado_%s.%s", timestamp, formato.toLowerCase());
    }

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

    // =============== MÉTODOS PÚBLICOS DE INFORMACIÓN ===============


    /**
     * Valida si una consulta puede ser consolidada
     */
    public boolean puedeSerConsolidada(ParametrosFiltrosDTO filtros) {
        return consolidacionService.validarConsolidacion(filtros);
    }
}