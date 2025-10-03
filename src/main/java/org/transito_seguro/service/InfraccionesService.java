package org.transito_seguro.service;

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
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.factory.RepositoryFactory;

import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
                        log.error("ExecutorService no se pudo cerrar");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("InfraccionesService cerrado");
    }

    // =============== API PÚBLICA PRINCIPAL ===============

    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("Ejecutando consulta: {}", tipoConsulta);
        validarTipoConsulta(tipoConsulta);
        return ejecutarQueryDesdeBaseDatos(tipoConsulta, consulta);
    }

    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta)
            throws ValidationException {
        log.info("Descargando archivo: {}", tipoConsulta);
        validarTipoConsulta(tipoConsulta);
        return consultarInfraccionesComoArchivo(consulta, tipoConsulta);
    }

    public List<InfraccionesRepositoryImpl> determinarRepositories(ParametrosFiltrosDTO filtros) {
        if (filtros != null && filtros.esConsolidado()) {
            log.debug("Consolidación activa - usando todos los repositorios");
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }

        if (filtros != null && Boolean.TRUE.equals(filtros.getUsarTodasLasBDS())) {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }

        if (filtros != null && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            List<String> provinciasNormalizadas = validator.normalizarProvincias(filtros.getBaseDatos());
            return provinciasNormalizadas.stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(provincia -> (InfraccionesRepositoryImpl) repositoryFactory.getRepository(provincia))
                    .collect(Collectors.toList());
        }

        return repositoryFactory.getAllRepositories().values().stream()
                .map(repo -> (InfraccionesRepositoryImpl) repo)
                .collect(Collectors.toList());
    }

    public boolean puedeSerConsolidada(ParametrosFiltrosDTO filtros) {
        return consolidacionService.validarConsolidacion(filtros);
    }

    // =============== CORE: EJECUCIÓN DESDE BASE DE DATOS ===============

    public Object ejecutarQueryDesdeBaseDatos(String codigoQuery, ConsultaQueryDTO consulta)
            throws ValidationException {

        log.info("Ejecutando query BD: {} - Consolidado: {}",
                codigoQuery, consulta.getParametrosFiltros() != null &&
                        consulta.getParametrosFiltros().esConsolidado());

        QueryStorage queryStorage = obtenerYValidarQuery(codigoQuery);
        registrarUsoQuery(queryStorage);
        validator.validarConsulta(consulta);

        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No hay repositorios válidos para: {}", codigoQuery);
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        return ejecutarSegunTipoProcesamiento(queryStorage, repositories, consulta);
    }

    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(ConsultaQueryDTO consulta, String nombreQuery)
            throws ValidationException {

        log.info("Generando archivo para: {}", nombreQuery);
        validator.validarConsulta(consulta);

        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        if (consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado()) {
            log.info("Archivo consolidado CSV - {} provincias", repositories.size());
            return generarArchivoConsolidadoCSV(repositories, consulta, nombreQuery);
        }

        log.info("Archivo normal - {} repositorios, formato: {}", repositories.size(), formato);
        return generarArchivoNormal(repositories, consulta, nombreQuery, formato);
    }

    // =============== MÉTODOS PRIVADOS: VALIDACIÓN ===============

    private void validarTipoConsulta(String tipoConsulta) throws ValidationException {
        List<String> nombresQuerys = queryStorageRepository.findAllbyCodigo();

        if (nombresQuerys.isEmpty()) {
            throw new ValidationException("No hay queries en base de datos");
        }

        if (!nombresQuerys.contains(tipoConsulta)) {
            throw new ValidationException("Query '" + tipoConsulta + "' no existe. " +
                    "Disponibles: " + nombresQuerys);
        }
    }

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

    private void registrarUsoQuery(QueryStorage queryStorage) {
        try {
            queryStorage.registrarUso();
            queryStorageRepository.save(queryStorage);
            log.debug("Uso registrado: {}", queryStorage.getCodigo());
        } catch (Exception e) {
            log.warn("Error registrando uso {}: {}", queryStorage.getCodigo(), e.getMessage());
        }
    }

    // =============== LÓGICA DE DECISIÓN DE ESTRATEGIA ===============

    private Object ejecutarSegunTipoProcesamiento(QueryStorage queryStorage,
                                                  List<InfraccionesRepositoryImpl> repositories,
                                                  ConsultaQueryDTO consulta) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        boolean esConsolidado = filtros != null &&
                filtros.esConsolidado() &&
                esQueryConsolidable(queryStorage);

        boolean usarLotes = debeUsarLotes(filtros, repositories);

        log.info("Estrategia - Consolidado: {}, Lotes: {}", esConsolidado, usarLotes);

        if (esConsolidado && usarLotes) {
            return ejecutarConsolidacionConLotes(queryStorage, repositories, consulta);
        }
        else if (esConsolidado) {
            return ejecutarConsolidacionSecuencial(queryStorage, repositories, consulta);
        }
        else if (usarLotes) {
            return ejecutarConLotes(queryStorage, repositories, consulta);
        }
        else {
            return ejecutarQueryStorageNormal(queryStorage, repositories, consulta);
        }
    }

    private boolean esQueryConsolidable(QueryStorage queryStorage) {
        return cacheQueryConsolidable.computeIfAbsent(queryStorage.getCodigo(),
                key -> queryStorage.getEsConsolidable());
    }

    private boolean debeUsarLotes(ParametrosFiltrosDTO filtros, List<InfraccionesRepositoryImpl> repositories) {
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

    // =============== EJECUCIÓN: LOTES ===============

    private Object ejecutarConLotes(QueryStorage queryStorage,
                                    List<InfraccionesRepositoryImpl> repositories,
                                    ConsultaQueryDTO consulta) {

        List<Map<String, Object>> resultados = Collections.synchronizedList(new ArrayList<>());

        batchProcessor.procesarEnLotes(
                repositories,
                consulta.getParametrosFiltros(),
                queryStorage.getCodigo(),
                resultados::addAll
        );

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultados, formato);
    }

    // =============== EJECUCIÓN: CONSOLIDACIÓN ===============

    private Object ejecutarConsolidacionConLotes(QueryStorage queryStorage,
                                                 List<InfraccionesRepositoryImpl> repositories,
                                                 ConsultaQueryDTO consulta) {

        log.info("Consolidación progresiva con BatchProcessor");

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Preparar estructura para consolidación thread-safe
        ConcurrentHashMap<String, Map<String, Object>> gruposConsolidados = new ConcurrentHashMap<>();
        List<String> camposAgrupacion = determinarCamposAgrupacionDesdeBD(filtros, queryStorage);
        List<String> camposNumericos = queryStorage.getCamposNumericosList();

        AtomicInteger registrosProcesados = new AtomicInteger(0);

        log.info("Campos agrupación: {}, Campos numéricos: {}", camposAgrupacion, camposNumericos);

        // Procesar y consolidar sobre la marcha
        batchProcessor.procesarEnLotes(
                repositories,
                filtros,
                queryStorage.getCodigo(),
                lote -> {
                    if (lote == null || lote.isEmpty()) return;

                    // Normalizar y consolidar cada registro del lote
                    for (Map<String, Object> registro : lote) {
                        // Normalizar provincia
                        String provincia = obtenerProvinciaDelRegistro(registro);
                        String provinciaNorm = org.transito_seguro.utils.NormalizadorProvincias.normalizar(provincia);
                        registro.put("provincia", provinciaNorm);
                        registro.put("provincia_origen", provinciaNorm);

                        // Crear clave de grupo
                        String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

                        // Consolidar de forma thread-safe
                        gruposConsolidados.compute(claveGrupo, (key, grupoExistente) -> {
                            if (grupoExistente == null) {
                                // Primer registro del grupo
                                return crearGrupoNuevo(registro, camposAgrupacion, camposNumericos);
                            } else {
                                // Sumar al grupo existente
                                sumarCamposNumericosThreadSafe(grupoExistente, registro, camposNumericos);
                                return grupoExistente;
                            }
                        });
                    }

                    int procesados = registrosProcesados.addAndGet(lote.size());
                    if (procesados % 50000 == 0) {
                        log.debug("Procesados {} registros -> {} grupos consolidados",
                                procesados, gruposConsolidados.size());
                    }
                }
        );

        log.info("BatchProcessor procesó {} registros -> {} grupos consolidados",
                registrosProcesados.get(), gruposConsolidados.size());

        if (gruposConsolidados.isEmpty()) {
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        List<Map<String, Object>> resultado = new ArrayList<>(gruposConsolidados.values());
        log.info("Consolidación completada: {} grupos finales", resultado.size());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultado, formato);
    }

    /**
     * Versión thread-safe de sumarCamposNumericos
     * No necesita synchronized porque se usa dentro de compute() que ya es atómico
     */
    private void sumarCamposNumericosThreadSafe(Map<String, Object> grupo,
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

    private Object ejecutarConsolidacionSecuencial(QueryStorage queryStorage,
                                                   List<InfraccionesRepositoryImpl> repositories,
                                                   ConsultaQueryDTO consulta) {
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        try {
            // Usar un Map para consolidar sobre la marcha
            Map<String, Map<String, Object>> gruposConsolidados = new LinkedHashMap<>();
            List<String> camposAgrupacion = determinarCamposAgrupacionDesdeBD(filtros, queryStorage);
            List<String> camposNumericos = queryStorage.getCamposNumericosList();

            log.info("Consolidación progresiva - Agrupación: {}, Numéricos: {}",
                    camposAgrupacion, camposNumericos);

            // Procesar provincia por provincia
            for (InfraccionesRepositoryImpl repo : repositories) {
                String provincia = repo.getProvincia();

                try {
                    String sqlDinamico = queryStorage.getSqlQuery();
                    ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                            sqlDinamico, filtros);

                    List<Map<String, Object>> datosProvincia = repo.getNamedParameterJdbcTemplate().queryForList(
                            resultado.getQueryModificada(),
                            resultado.getParametros()
                    );

                    if (datosProvincia != null && !datosProvincia.isEmpty()) {
                        // Normalizar provincia
                        for (Map<String, Object> registro : datosProvincia) {
                            String prov = org.transito_seguro.utils.NormalizadorProvincias.normalizar(provincia);
                            registro.put("provincia", prov);
                            registro.put("provincia_origen", prov);

                            // Consolidar inmediatamente
                            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

                            Map<String, Object> grupo = gruposConsolidados.computeIfAbsent(claveGrupo,
                                    k -> crearGrupoNuevo(registro, camposAgrupacion, camposNumericos));

                            sumarCamposNumericos(grupo, registro, camposNumericos);
                        }

                        log.debug("Provincia {} procesada: {} registros -> {} grupos acumulados",
                                provincia, datosProvincia.size(), gruposConsolidados.size());

                        // Liberar memoria de provincia
                        datosProvincia.clear();
                    }

                } catch (Exception e) {
                    log.error("Error en provincia {}: {}", provincia, e.getMessage());
                }
            }

            List<Map<String, Object>> resultado = new ArrayList<>(gruposConsolidados.values());
            log.info("Consolidación completada: {} grupos finales", resultado.size());

            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return formatoConverter.convertir(resultado, formato);

        } catch (Exception e) {
            log.error("Error consolidación '{}': {}", queryStorage.getCodigo(), e.getMessage(), e);
            throw new RuntimeException("Error en consolidación", e);
        }
    }

    private List<Map<String, Object>> recopilarDatosConQueryStorage(QueryStorage queryStorage,
                                                                    List<InfraccionesRepositoryImpl> repositories,
                                                                    ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                String sqlDinamico = queryStorage.getSqlQuery();
                ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                        sqlDinamico, filtros);

                List<Map<String, Object>> datosProvider = repo.getNamedParameterJdbcTemplate().queryForList(
                        resultado.getQueryModificada(),
                        resultado.getParametros()
                );

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                        registro.put("query_codigo", queryStorage.getCodigo());
                    }

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Query '{}' en {}: {} registros",
                            queryStorage.getCodigo(), provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error QueryStorage '{}' en {}: {}",
                        queryStorage.getCodigo(), provincia, e.getMessage());
            }
        }

        log.info("QueryStorage '{}': {} registros de {} provincias",
                queryStorage.getCodigo(), todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    private List<Map<String, Object>> consolidarConMetadataDeBaseDatos(List<Map<String, Object>> datos,
                                                                       ParametrosFiltrosDTO filtros,
                                                                       QueryStorage queryStorage) {

        if (datos.isEmpty()) return Collections.emptyList();

        List<String> camposAgrupacion = determinarCamposAgrupacionDesdeBD(filtros, queryStorage);
        List<String> camposNumericos = queryStorage.getCamposNumericosList();

        log.info("Consolidación - Query: {}, Agrupación: {}, Numéricos: {}",
                queryStorage.getCodigo(), camposAgrupacion, camposNumericos);

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    private List<String> determinarCamposAgrupacionDesdeBD(ParametrosFiltrosDTO filtros, QueryStorage queryStorage) {

        List<String> camposAgrupacion = new ArrayList<>();
        List<String> consolidacionUsuario = filtros.getConsolidacionSeguro();

        if (!consolidacionUsuario.isEmpty()) {
            List<String> camposValidosBD = queryStorage.getCamposAgrupacionList();

            for (String campo : consolidacionUsuario) {
                if (camposValidosBD.contains(campo)) {
                    camposAgrupacion.add(campo);
                } else {
                    log.warn("Campo '{}' no válido para '{}'", campo, queryStorage.getCodigo());
                }
            }
        }

        if (camposAgrupacion.isEmpty()) {
            camposAgrupacion.addAll(queryStorage.getCamposAgrupacionList());
            log.info("Usando campos BD para '{}': {}", queryStorage.getCodigo(), camposAgrupacion);
        }

        if (!camposAgrupacion.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        return camposAgrupacion.stream().distinct().collect(Collectors.toList());
    }

    // =============== EJECUCIÓN: NORMAL ===============

    private Object ejecutarQueryStorageNormal(QueryStorage queryStorage,
                                              List<InfraccionesRepositoryImpl> repositories,
                                              ConsultaQueryDTO consulta) throws ValidationException {

        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();
                        String sqlDinamico = queryStorage.getSqlQuery();

                        ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(
                                sqlDinamico, consulta.getParametrosFiltros());

                        List<Map<String, Object>> datos = repo.getNamedParameterJdbcTemplate().queryForList(
                                resultado.getQueryModificada(),
                                resultado.getParametros()
                        );

                        datos.forEach(registro -> {
                            registro.put("provincia", provincia);
                            registro.put("query_codigo", queryStorage.getCodigo());
                        });

                        return datos.stream();

                    } catch (Exception e) {
                        log.error("Error QueryStorage '{}' en {}: {}",
                                queryStorage.getCodigo(), repo.getProvincia(), e.getMessage());
                        return java.util.stream.Stream.empty();
                    }
                })
                .collect(Collectors.toList());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    // =============== GENERACIÓN DE ARCHIVOS ===============

    private ResponseEntity<byte[]> generarArchivoConsolidadoCSV(
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta,
            String nombreQuery) {

        try {
            Path tempFile = Files.createTempFile("consolidado_", ".csv");

            try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {

                AtomicBoolean headerEscrito = new AtomicBoolean(false);
                AtomicLong registrosEscritos = new AtomicLong(0);

                batchProcessor.procesarEnLotes(
                        repositories,
                        consulta.getParametrosFiltros(),
                        nombreQuery,
                        lote -> {
                            if (lote == null || lote.isEmpty()) return;

                            try {
                                synchronized(writer) {
                                    if (!headerEscrito.get() && !lote.isEmpty()) {
                                        writer.write(String.join(",", lote.get(0).keySet()));
                                        writer.newLine();
                                        headerEscrito.set(true);
                                    }

                                    for (Map<String, Object> registro : lote) {
                                        writer.write(registro.values().stream()
                                                .map(v -> v != null ? escaparCSV(v.toString()) : "")
                                                .collect(Collectors.joining(",")));
                                        writer.newLine();
                                    }

                                    registrosEscritos.addAndGet(lote.size());
                                }
                            } catch (IOException e) {
                                try {
                                    throw new RuntimesFormatoExceleException("Error escribiendo CSV", e);
                                } catch (RuntimesFormatoExceleException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }
                );

                log.info("CSV: {} registros", registrosEscritos.get());
            }

            byte[] contenido = Files.readAllBytes(tempFile);
            Files.delete(tempFile);

            return construirRespuestaArchivo(contenido,
                    generarNombreArchivoConsolidado("csv"), "csv", repositories.size());

        } catch (Exception e) {
            log.error("Error generando CSV: {}", e.getMessage(), e);
            throw new RuntimeException("Error generando CSV", e);
        }
    }

    private String escaparCSV(String valor) {
        if (valor.contains(",") || valor.contains("\"") || valor.contains("\n")) {
            return "\"" + valor.replace("\"", "\"\"") + "\"";
        }
        return valor;
    }

    private ResponseEntity<byte[]> generarArchivoNormal(
            List<InfraccionesRepositoryImpl> repositories,
            ConsultaQueryDTO consulta,
            String nombreQuery,
            String formato) {

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            context = streamingConverter.inicializarStreaming(formato, outputStream);
            if (context == null) {
                throw new RuntimeException("No se pudo inicializar streaming para " + formato);
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            if (esFormatoExcel(formato)) {
                log.info("Excel - procesamiento SECUENCIAL");
                batchProcessor.procesarEnLotes(
                        repositories,
                        consulta.getParametrosFiltros(),
                        nombreQuery,
                        lote -> procesarLoteParaArchivo(finalContext, lote)
                );
            } else {
                log.info("Formato {} - procesamiento automático", formato);
                batchProcessor.procesarEnLotes(
                        repositories,
                        consulta.getParametrosFiltros(),
                        nombreQuery,
                        lote -> procesarLoteParaArchivo(finalContext, lote)
                );
            }

            streamingConverter.finalizarStreaming(context);

            return construirRespuestaArchivo(
                    outputStream.toByteArray(),
                    generarNombreArchivo(formato),
                    formato,
                    repositories.size()
            );

        } catch (Exception e) {
            log.error("Error generando archivo: {}", e.getMessage(), e);
            limpiarContextoStreaming(context);
            throw new RuntimeException("Error generando archivo", e);
        }
    }

    private void procesarLoteParaArchivo(StreamingFormatoConverter.StreamingContext context,
                                         List<Map<String, Object>> lote) {
        try {
            if (lote != null && !lote.isEmpty()) {
                streamingConverter.procesarLoteStreaming(context, lote);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error procesando lote", e);
        }
    }

    private boolean esFormatoExcel(String formato) {
        if (formato == null) return false;
        String f = formato.toLowerCase();
        return f.equals("excel") || f.equals("xlsx") || f.equals("xls");
    }

    // =============== UTILIDADES ===============

    private ResponseEntity<byte[]> construirRespuestaArchivo(byte[] data, String filename,
                                                             String formato, int numProvincias) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentDisposition(
                ContentDisposition.builder("attachment")
                        .filename(filename, StandardCharsets.UTF_8)
                        .build()
        );
        headers.setContentType(determinarMediaType(formato));
        headers.set("X-Total-Provincias", String.valueOf(numProvincias));
        headers.set("X-Archivo-Tamano", String.valueOf(data.length));

        log.info("Archivo: {} bytes, {} provincias, {}", data.length, numProvincias, formato);
        return ResponseEntity.ok().headers(headers).body(data);
    }

    private void limpiarContextoStreaming(StreamingFormatoConverter.StreamingContext context) {
        if (context != null) {
            try {
                streamingConverter.finalizarStreaming(context);
            } catch (Exception e) {
                log.warn("Error limpiando streaming: {}", e.getMessage());
            }
        }
    }

    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = org.transito_seguro.utils.NormalizadorProvincias.normalizar(provincia);
            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }
        return datos;
    }

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

    private List<Map<String, Object>> consolidarPorCampos(List<Map<String, Object>> datos,
                                                          List<String> camposAgrupacion,
                                                          List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("Sin campos agrupación, datos sin consolidar");
            return datos;
        }

        log.info("Consolidando {} registros por: {}", datos.size(), camposAgrupacion);

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoNuevo(registro, camposAgrupacion, camposNumericos));

            sumarCamposNumericos(grupo, registro, camposNumericos);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());
        log.info("Consolidación: {} -> {} grupos", datos.size(), resultado.size());
        return resultado;
    }

    private Map<String, Object> crearGrupoNuevo(Map<String, Object> registro,
                                                List<String> camposAgrupacion,
                                                List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

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

    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

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

    private Long obtenerValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
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
            case "xlsx":
                return MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            case "json":
            default:
                return MediaType.APPLICATION_JSON;
        }
    }
}