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
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.wrapper.CursorResponseWrapper;


import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.ByteArrayOutputStream;
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
    private StreamingFormatoConverter streamingConverter;

    @Value("${app.cursor.default-page-size:50}")
    private int defaultPageSize;

    @Value("${app.cursor.max-page-size:1000}")
    private int maxPageSize;

    @Value("${app.cursor.default-type:fecha_id}")
    private String defaultCursorType;

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

    /**
     * Método principal - SOLO CURSOR
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    /**
     * Método genérico que reemplaza todos los métodos específicos
     */
    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfracciones(consulta, nombreQuery);
    }

    /**
     * Método genérico para descarga de archivos
     */
    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfraccionesComoArchivo(consulta, nombreQuery);
    }

    /**
     * Método principal simplificado - SOLO CURSOR (reemplaza lógica anterior)
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        log.info("Iniciando consulta con CURSOR: {}", nombreQuery);

        // 1. Validar consulta
        validator.validarConsulta(consulta);

        // 2. Normalizar parámetros de cursor
        consulta = normalizarParametrosCursor(consulta);

        // 3. Determinar repositorios
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No se encontraron repositorios válidos para la consulta");
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // 4. Ejecutar con cursor (elimina toda la lógica de estimación y lotes)
        return ejecutarConsultaCursor(repositories, consulta, nombreQuery);
    }

    /**
     * Normaliza parámetros de cursor, estableciendo defaults si no se especifican
     */
    private ConsultaQueryDTO normalizarParametrosCursor(ConsultaQueryDTO consulta) {
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        if (filtros == null) {
            filtros = new ParametrosFiltrosDTO();
        }

        // Establecer valores por defecto para cursor
        if (filtros.getTipoCursor() == null) {
            filtros.setTipoCursor(defaultCursorType);
        }

        if (filtros.getPageSize() == null || filtros.getPageSize() <= 0) {
            filtros.setPageSize(defaultPageSize);
        } else if (filtros.getPageSize() > maxPageSize) {
            log.warn("PageSize {} excede el máximo {}, limitando", filtros.getPageSize(), maxPageSize);
            filtros.setPageSize(maxPageSize);
        }

        if (filtros.getDireccion() == null) {
            filtros.setDireccion("forward");
        }

        // Validar parámetros
        if (!filtros.validarPaginacion()) {
            log.warn("Parámetros de cursor inválidos, aplicando defaults");
            filtros = ParametrosFiltrosDTO.primeraPagina(defaultPageSize, defaultCursorType);

            // Mantener filtros de búsqueda originales
            if (consulta.getParametrosFiltros() != null) {
                filtros = combinarFiltros(filtros, consulta.getParametrosFiltros());
            }
        }

        return ConsultaQueryDTO.builder()
                .formato(consulta.getFormato())
                .parametrosFiltros(filtros)
                .build();
    }

    /**
     * Ejecuta consulta usando SOLO cursor
     */
    private Object ejecutarConsultaCursor(List<InfraccionesRepositoryImpl> repositories,
                                          ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        log.info("Ejecutando consulta con cursor - Tipo: {}, PageSize: {}, Primera: {}",
                filtros.getTipoCursor(), filtros.getPageSizeEfectivo(), filtros.esPrimeraPagina());

        try {
            List<Map<String, Object>> resultadosCombinados = new ArrayList<>();

            // Procesar cada repositorio hasta obtener suficientes resultados
            for (InfraccionesRepositoryImpl repo : repositories) {
                try {
                    String provincia = repo.getProvincia();

                    // Ejecutar query - el ParametrosProcessor maneja toda la lógica de cursor
                    List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                    // Agregar provincia a cada registro
                    resultado.forEach(registro -> registro.put("provincia_origen", provincia));
                    resultadosCombinados.addAll(resultado);

                    log.debug("Cursor query ejecutada en {}: {} registros", provincia, resultado.size());

                    // Para cursor, típicamente limitamos por página, no por provincia total
                    if (resultadosCombinados.size() >= filtros.getLimiteSql()) {
                        break; // Ya tenemos suficiente para esta página
                    }

                } catch (Exception e) {
                    log.error("Error en provincia {} con cursor: {}", repo.getProvincia(), e.getMessage());
                    // Con cursor, no fallamos por una provincia - continuamos
                }
            }

            // Crear respuesta según formato
            if ("json".equals(formato)) {
                return crearRespuestaCursor(resultadosCombinados, filtros, repositories);
            } else {
                // Para CSV/Excel, devolver solo los datos (sin wrapper)
                List<Map<String, Object>> datosFinales = limitarResultados(resultadosCombinados, filtros);
                return formatoConverter.convertir(datosFinales, formato);
            }

        } catch (Exception e) {
            log.error("Error en consulta con cursor: {}", e.getMessage(), e);
            throw new RuntimeException("Error procesando consulta con cursor", e);
        }
    }

    /**
     * Crea respuesta con información de cursor para JSON
     */
    private CursorResponseWrapper<Map<String, Object>> crearRespuestaCursor(
            List<Map<String, Object>> resultados,
            ParametrosFiltrosDTO filtros,
            List<InfraccionesRepositoryImpl> repositories) {

        int pageSize = filtros.getPageSizeEfectivo();
        String tipoCursor = filtros.getTipoCursor();

        // Determinar si hay más páginas
        boolean hasNext = resultados.size() > pageSize;
        List<Map<String, Object>> datosFinales = hasNext ?
                resultados.subList(0, pageSize) : resultados;

        // Generar cursors para navegación
        String nextCursor = null;
        if (hasNext && !datosFinales.isEmpty()) {
            nextCursor = ParametrosFiltrosDTO.extraerCursorDeResultados(datosFinales, tipoCursor);
        }

        // Información de página anterior
        boolean hasPrev = !filtros.esPrimeraPagina();
        String prevCursor = hasPrev ? filtros.getCursor() : null;

        // Crear wrapper de respuesta
        CursorResponseWrapper.CursorPageInfo pageInfo = CursorResponseWrapper.CursorPageInfo.builder()
                .nextCursor(nextCursor)
                .prevCursor(prevCursor)
                .hasNext(hasNext)
                .hasPrev(hasPrev)
                .pageSize(pageSize)
                .currentPageSize(datosFinales.size())
                .cursorType(tipoCursor)
                .cursorInfo(crearInfoCursor(filtros))
                .build();

        // Metadata adicional
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("metodo_paginacion", "cursor");
        metadata.put("performance", "optimizada");
        metadata.put("provincias_consultadas", repositories.stream()
                .map(InfraccionesRepositoryImpl::getProvincia)
                .collect(Collectors.toList()));
        metadata.put("es_primera_pagina", filtros.esPrimeraPagina());
        metadata.put("direccion", filtros.getDireccion());

        return CursorResponseWrapper.<Map<String, Object>>builder()
                .datos(datosFinales)
                .pageInfo(pageInfo)
                .metadata(metadata)
                .build();
    }

    /**
     * Limita resultados al tamaño de página
     */
    private List<Map<String, Object>> limitarResultados(List<Map<String, Object>> resultados, ParametrosFiltrosDTO filtros) {
        int pageSize = filtros.getPageSizeEfectivo();
        return resultados.size() > pageSize ?
                resultados.subList(0, pageSize) : resultados;
    }

    /**
     * Crea información adicional del cursor para debugging
     */
    private Map<String, Object> crearInfoCursor(ParametrosFiltrosDTO filtros) {
        Map<String, Object> info = new HashMap<>();
        info.put("cursor_actual", filtros.getCursor());
        info.put("tipo", filtros.getTipoCursor());
        info.put("direccion", filtros.getDireccion());
        info.put("page_size_solicitado", filtros.getPageSize());
        info.put("page_size_efectivo", filtros.getPageSizeEfectivo());

        if (!filtros.esPrimeraPagina()) {
            String[] partesCursor = filtros.parsearCursor();
            if (partesCursor.length > 1) {
                info.put("cursor_fecha", partesCursor[0]);
                info.put("cursor_id", partesCursor[1]);
            } else {
                info.put("cursor_valor", partesCursor[0]);
            }
        }

        return info;
    }

    // =============== MÉTODOS PÚBLICOS PARA NAVEGACIÓN ===============

    /**
     * Obtener primera página (método público)
     */
    public CursorResponseWrapper<Map<String, Object>> obtenerPrimeraPagina(
            String tipoConsulta,
            String tipoCursor,
            int pageSize,
            ParametrosFiltrosDTO filtrosAdicionales) throws ValidationException {

        // Crear filtros para primera página
        ParametrosFiltrosDTO filtros = ParametrosFiltrosDTO.primeraPagina(pageSize, tipoCursor);

        // Combinar con filtros adicionales si existen
        if (filtrosAdicionales != null) {
            filtros = combinarFiltros(filtros, filtrosAdicionales);
        }

        ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                .formato("json")
                .parametrosFiltros(filtros)
                .build();

        Object resultado = ejecutarConsultaPorTipo(tipoConsulta, consulta);

        if (resultado instanceof CursorResponseWrapper) {
            return (CursorResponseWrapper<Map<String, Object>>) resultado;
        } else {
            log.warn("Resultado no era CursorResponseWrapper, creando wrapper vacío");
            return CursorResponseWrapper.empty(pageSize, tipoCursor);
        }
    }

    /**
     * Obtener siguiente página
     */
    public CursorResponseWrapper<Map<String, Object>> obtenerSiguientePagina(
            String tipoConsulta,
            String cursor,
            String tipoCursor,
            int pageSize,
            ParametrosFiltrosDTO filtrosAdicionales) throws ValidationException {

        // Crear filtros para siguiente página
        ParametrosFiltrosDTO filtros = ParametrosFiltrosDTO.siguientePagina(cursor, tipoCursor, pageSize);

        // Combinar con filtros adicionales si existen
        if (filtrosAdicionales != null) {
            filtros = combinarFiltros(filtros, filtrosAdicionales);
        }

        ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                .formato("json")
                .parametrosFiltros(filtros)
                .build();

        Object resultado = ejecutarConsultaPorTipo(tipoConsulta, consulta);

        if (resultado instanceof CursorResponseWrapper) {
            return (CursorResponseWrapper<Map<String, Object>>) resultado;
        } else {
            return CursorResponseWrapper.empty(pageSize, tipoCursor);
        }
    }

    /**
     * Obtener página anterior
     */
    public CursorResponseWrapper<Map<String, Object>> obtenerPaginaAnterior(
            String tipoConsulta,
            String cursor,
            String tipoCursor,
            int pageSize,
            ParametrosFiltrosDTO filtrosAdicionales) throws ValidationException {

        // Crear filtros para página anterior
        ParametrosFiltrosDTO filtros = ParametrosFiltrosDTO.paginaAnterior(cursor, tipoCursor, pageSize);

        // Combinar con filtros adicionales si existen
        if (filtrosAdicionales != null) {
            filtros = combinarFiltros(filtros, filtrosAdicionales);
        }

        ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                .formato("json")
                .parametrosFiltros(filtros)
                .build();

        Object resultado = ejecutarConsultaPorTipo(tipoConsulta, consulta);

        if (resultado instanceof CursorResponseWrapper) {
            return (CursorResponseWrapper<Map<String, Object>>) resultado;
        } else {
            return CursorResponseWrapper.empty(pageSize, tipoCursor);
        }
    }

    /**
     * Combina filtros de cursor con filtros adicionales
     */
    private ParametrosFiltrosDTO combinarFiltros(ParametrosFiltrosDTO filtrosCursor, ParametrosFiltrosDTO filtrosAdicionales) {
        return filtrosCursor.toBuilder()
                // Fechas
                .fechaInicio(filtrosAdicionales.getFechaInicio())
                .fechaFin(filtrosAdicionales.getFechaFin())
                .fechaEspecifica(filtrosAdicionales.getFechaEspecifica())

                // Ubicación
                .provincias(filtrosAdicionales.getProvincias())
                .municipios(filtrosAdicionales.getMunicipios())
                .lugares(filtrosAdicionales.getLugares())
                .partido(filtrosAdicionales.getPartido())
                .baseDatos(filtrosAdicionales.getBaseDatos())

                // Equipos
                .patronesEquipos(filtrosAdicionales.getPatronesEquipos())
                .tiposDispositivos(filtrosAdicionales.getTiposDispositivos())
                .seriesEquiposExactas(filtrosAdicionales.getSeriesEquiposExactas())

                // Infracciones
                .concesiones(filtrosAdicionales.getConcesiones())
                .tiposInfracciones(filtrosAdicionales.getTiposInfracciones())
                .estadosInfracciones(filtrosAdicionales.getEstadosInfracciones())
                .exportadoSacit(filtrosAdicionales.getExportadoSacit())

                // Vehículos
                .tipoVehiculo(filtrosAdicionales.getTipoVehiculo())
                .tieneEmail(filtrosAdicionales.getTieneEmail())
                .tipoDocumento(filtrosAdicionales.getTipoDocumento())

                // Otros
                .usarTodasLasBDS(filtrosAdicionales.getUsarTodasLasBDS())
                .filtrosAdicionales(filtrosAdicionales.getFiltrosAdicionales())

                .build();
    }

    // =============== MÉTODO PARA BATCH PROCESSING CON CURSOR ===============

    /**
     * Procesa TODOS los datos usando cursor (reemplaza batchProcessor anterior)
     */
    public void procesarTodosLosRegistrosConCursor(
            String tipoConsulta,
            ParametrosFiltrosDTO filtrosBase,
            java.util.function.Consumer<List<Map<String, Object>>> procesadorLote,
            int pageSize) throws ValidationException {

        String cursor = null;
        int totalProcesados = 0;
        int iteraciones = 0;
        final int maxIteraciones = 1000; // Límite de seguridad

        log.info("Iniciando procesamiento completo con cursor - PageSize: {}", pageSize);

        do {
            try {
                // Crear filtros para esta iteración
                ParametrosFiltrosDTO filtros = cursor == null ?
                        ParametrosFiltrosDTO.primeraPagina(pageSize, defaultCursorType) :
                        ParametrosFiltrosDTO.siguientePagina(cursor, defaultCursorType, pageSize);

                // Combinar con filtros base
                if (filtrosBase != null) {
                    filtros = combinarFiltros(filtros, filtrosBase);
                }

                // Ejecutar consulta
                CursorResponseWrapper<Map<String, Object>> resultado =
                        cursor == null ?
                                obtenerPrimeraPagina(tipoConsulta, defaultCursorType, pageSize, filtrosBase) :
                                obtenerSiguientePagina(tipoConsulta, cursor, defaultCursorType, pageSize, filtrosBase);

                if (resultado.getDatos().isEmpty()) {
                    log.info("No hay más datos para procesar");
                    break;
                }

                // Procesar este lote
                procesadorLote.accept(resultado.getDatos());

                // Actualizar contadores
                totalProcesados += resultado.getDatos().size();
                cursor = resultado.getPageInfo().getNextCursor();
                iteraciones++;

                log.info("Lote procesado: {} registros, Total: {}, Iteración: {}",
                        resultado.getDatos().size(), totalProcesados, iteraciones);

                // Verificar límites de seguridad
                if (iteraciones >= maxIteraciones) {
                    log.warn("Alcanzado límite máximo de iteraciones ({}), deteniendo", maxIteraciones);
                    break;
                }

                // Pequeña pausa para no sobrecargar
                Thread.sleep(50);

            } catch (Exception e) {
                log.error("Error en iteración {}: {}", iteraciones, e.getMessage(), e);
                break;
            }

        } while (cursor != null);

        log.info("Procesamiento completo finalizado - Total: {} registros en {} iteraciones",
                totalProcesados, iteraciones);
    }

    // =============== MÉTODOS PARA DESCARGA DE ARCHIVOS (SIN CAMBIOS MAYORES) ===============

    /**
     * Versión que retorna ResponseEntity para archivos grandes
     */
    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(ConsultaQueryDTO consulta, String nombreQuery)
            throws ValidationException {

        log.info("Generando archivo para consulta con query: {}", nombreQuery);

        validator.validarConsulta(consulta);
        consulta = normalizarParametrosCursor(consulta);
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming para archivo");
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            // Procesar con cursor (más eficiente que el batch anterior)
            procesarTodosLosRegistrosConCursor(
                    obtenerTipoConsultaDesdeQuery(nombreQuery),
                    consulta.getParametrosFiltros(),
                    lote -> {
                        try {
                            if (lote != null && !lote.isEmpty()) {
                                streamingConverter.procesarLoteStreaming(finalContext, lote);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error procesando lote para archivo", e);
                        }
                    },
                    1000 // Page size grande para archivos
            );

            streamingConverter.finalizarStreaming(context);

            // Preparar headers HTTP
            HttpHeaders headers = new HttpHeaders();
            String filename = generarNombreArchivo(formato);
            headers.setContentDispositionFormData("attachment", filename);

            MediaType mediaType;
            switch (formato.toLowerCase()) {
                case "csv":
                    mediaType = MediaType.parseMediaType("text/csv");
                    break;
                case "excel":
                    mediaType = MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                    break;
                case "json":
                default:
                    mediaType = MediaType.APPLICATION_JSON;
                    break;
            }
            headers.setContentType(mediaType);

            byte[] responseData = outputStream.toByteArray();
            log.info("Archivo generado exitosamente: {} bytes", responseData.length);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(responseData);

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

    // =============== MÉTODOS UTILITARIOS ===============

    private String obtenerTipoConsultaDesdeQuery(String nombreQuery) {
        // Buscar en el mapeo inverso
        return QUERY_MAPPING.entrySet().stream()
                .filter(entry -> entry.getValue().equals(nombreQuery))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse("personas-juridicas"); // Default
    }

    private String generarNombreArchivo(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_%s.%s", timestamp, formato.toLowerCase());
    }

    /**
     * REEMPLAZA: determinarRepositories - sin cambios de lógica pero actualizado comentario
     */
    private List<InfraccionesRepositoryImpl> determinarRepositories(ParametrosFiltrosDTO filtros) {
        // Esta lógica se mantiene igual, pero ahora trabaja con cursor en lugar de offset
        if (filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()) {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        } else if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
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
}