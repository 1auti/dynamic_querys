package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.FormatoConverter;
import org.transito_seguro.component.PaginationStrategyAnalyzer;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.component.DynamicBuilderQuery;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.QueryStorageDTO;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.model.query.QueryResult;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import javax.xml.bind.ValidationException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 *  Servicio para gestionar queries almacenadas en base de datos
 * Integra con el sistema de consolidación dinámico existente
 */
@Slf4j
@Service
@Transactional
public class DatabaseQueryService {

    @Autowired
    private QueryStorageRepository queryRepository;

    @Autowired
    private QueryAnalyzer queryAnalyzer;

    @Autowired
    private PaginationStrategyAnalyzer paginationStrategyAnalyzer;

    @Autowired
    private DynamicBuilderQuery builderQuery;

    @Autowired
    private ConsolidacionService consolidacionService;

    @Autowired
    private InfraccionesService infraccionesService;

    @Autowired
    private FormatoConverter formatoConverter;

    @Autowired
    private ParametrosProcessor parametrosProcessor;


    // =============== GESTIÓN DE QUERIES ===============

    /**
     * 💾 Guardar nueva query en base de datos
     */
    public QueryStorage guardarQuery(QueryStorageDTO dto) {
        log.info("Guardando nueva query: {}", dto.getCodigo());



        // Validaciones
        if (!dto.esValida()) {
            throw new IllegalArgumentException("Query no válida: faltan campos obligatorios");
        }

        // Verificar que el código no exista
        if (queryRepository.findByCodigo(dto.getCodigo()).isPresent()) {
            throw new IllegalArgumentException("Ya existe una query con código: " + dto.getCodigo());
        }

        String sql = builderQuery.construirSqlInteligente(dto.getSqlQuery());


        AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(sql);

        AnalisisPaginacion  analisisPaginacion = paginationStrategyAnalyzer.determinarEstrategia(sql);

        // Los filtros dinámicos se agregan solo al ejecutar
        QueryStorage query = QueryStorage.builder()
                .codigo(dto.getCodigo())
                .nombre(dto.getNombre())
                .descripcion(dto.getDescripcion())
                .sqlQuery(sql)
                .categoria(dto.getCategoria() != null ? dto.getCategoria() : "GENERAL")
                .esConsolidable(analisis.isEsConsolidable())
                .tipoConsolidacion(analisis.getTipoConsolidacion())
                .estrategiaPaginacion(analisisPaginacion.getEstrategiaPaginacion())
                .registrosEstimados(analisis.getRegistrosEstimados())
                .timeoutSegundos(dto.getTimeoutSegundos())
                .limiteMaximo(dto.getLimiteMaximo())
                .creadoPor(dto.getCreadoPor())
                .activa(dto.getActiva())
                .estado(EstadoQuery.ANALIZADA)
                .build();

        // Aplicar análisis automático o manual
        if (analisis.isEsConsolidable()) {
            query.setCamposAgrupacionList(analisis.getCamposAgrupacion());
            query.setCamposNumericosList(analisis.getCamposNumericos());
            query.setCamposUbicacionList(analisis.getCamposUbicacion());
            query.setCamposTiempoList(analisis.getCamposTiempo());
        } else if (dto.getEsConsolidable()) {
            // Usar metadata manual del DTO
            query.setEsConsolidable(true);
            query.setCamposAgrupacionList(dto.getCamposAgrupacion());
            query.setCamposNumericosList(dto.getCamposNumericos());
            query.setCamposUbicacionList(dto.getCamposUbicacion());
            query.setCamposTiempoList(dto.getCamposTiempo());
        }

        // Tags
        if (dto.getTags() != null) {
            query.setTagsList(dto.getTags());
        }

        query.analizarYMarcarConsolidable();

        QueryStorage guardada = queryRepository.save(query);

        log.info("Query '{}' guardada exitosamente - ID: {}, Consolidable: {}, Campos detectados: {}",
                guardada.getCodigo(), guardada.getId(), guardada.getEsConsolidable(),
                analisis.getTipoPorCampo().size());

        return guardada;
    }

   
    /**
     * ✏ Actualizar query existente
     */
    public QueryStorage actualizarQuery(String codigo, QueryStorageDTO dto) {
        log.info("Actualizando query: {}", codigo);

        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        // Actualizar campos
        query.setNombre(dto.getNombre());
        query.setDescripcion(dto.getDescripcion());
        query.setCategoria(dto.getCategoria());
        query.setTimeoutSegundos(dto.getTimeoutSegundos());
        query.setLimiteMaximo(dto.getLimiteMaximo());
        query.setActiva(dto.getActiva());

        // Si cambió el SQL, re-analizar
        if (!query.getSqlQuery().equals(dto.getSqlQuery())) {
            query.setSqlQuery(dto.getSqlQuery());
            query.setVersion(query.getVersion() + 1);

            // Re-análisis automático
            AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(dto.getSqlQuery());
            query.setEsConsolidable(analisis.isEsConsolidable());

            if (analisis.isEsConsolidable()) {
                query.setCamposAgrupacionList(analisis.getCamposAgrupacion());
                query.setCamposNumericosList(analisis.getCamposNumericos());
                query.setCamposUbicacionList(analisis.getCamposUbicacion());
                query.setCamposTiempoList(analisis.getCamposTiempo());
            }
        }

        // Actualizar metadata manual si se proporciona
        if (dto.getEsConsolidable()) {
            query.setEsConsolidable(true);
            if (dto.getCamposAgrupacion() != null) {
                query.setCamposAgrupacionList(dto.getCamposAgrupacion());
            }
            if (dto.getCamposNumericos() != null) {
                query.setCamposNumericosList(dto.getCamposNumericos());
            }
        }

        // Tags
        if (dto.getTags() != null) {
            query.setTagsList(dto.getTags());
        }

        return queryRepository.save(query);
    }

    /**
     *  Eliminar query (soft delete)
     */
    public void eliminarQuery(String codigo) {
        log.info("Eliminando query: {}", codigo);

        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        query.setActiva(false);
        query.setEstado(EstadoQuery.OBSOLETA);
        queryRepository.save(query);
    }

    // =============== EJECUCIÓN DE QUERIES ===============

    /**
     *  MÉTODO PRINCIPAL: Ejecutar query desde base de datos
     * Integra automáticamente con consolidación dinámica
     */
    public Object ejecutarQueryPorCodigo(String codigo, ConsultaQueryDTO consulta) {
        log.info("Ejecutando query desde BD: {} - Consolidado: {}",
                codigo, consulta.getParametrosFiltros() != null &&
                        consulta.getParametrosFiltros().esConsolidado());

        // 1. Obtener query de la base de datos
        QueryStorage queryStorage = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        if (!queryStorage.estaLista()) {
            throw new IllegalStateException("Query no está disponible: " + codigo);
        }

        // 2. Registrar uso
        queryStorage.registrarUso();
        queryRepository.save(queryStorage);

        try {
            // 3. Ejecutar según consolidación
            if (consulta.getParametrosFiltros() != null &&
                    consulta.getParametrosFiltros().esConsolidado() &&
                    queryStorage.getEsConsolidable()) {

                return ejecutarQueryConsolidada(queryStorage, consulta);
            } else {
                return ejecutarQueryNormal(queryStorage, consulta);
            }

        } catch (Exception e) {
            log.error("Error ejecutando query '{}': {}", codigo, e.getMessage(), e);
            throw new RuntimeException("Error ejecutando query desde BD", e);
        }
    }

    /**
     *  Ejecutar query con consolidación dinámica
     */
    private Object ejecutarQueryConsolidada(QueryStorage queryStorage, ConsultaQueryDTO consulta) {
        log.info("Ejecutando query CONSOLIDADA desde BD: {}", queryStorage.getCodigo());

        // Usar el sistema de consolidación existente pero con SQL de BD
        return ejecutarConsolidacionConQueryStorage(queryStorage, consulta);
    }

    /**
     *  Ejecutar query normal (sin consolidación)
     */
    private Object ejecutarQueryNormal(QueryStorage queryStorage, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("Ejecutando query NORMAL desde BD: {}", queryStorage.getCodigo());

        // Ejecutar usando el servicio existente simulando un archivo
        return ejecutarQueryComoArchivo(queryStorage, consulta);
    }

    // =============== INTEGRACIÓN CON SISTEMA EXISTENTE ===============

    /**
     *  Ejecuta consolidación usando QueryStorage en lugar de archivo
     */
    private Object ejecutarConsolidacionConQueryStorage(QueryStorage queryStorage, ConsultaQueryDTO consulta) {
        log.info("Ejecutando consolidación con QueryStorage: {}", queryStorage.getCodigo());

        // 1. Obtener repositorios para consolidación
        List<InfraccionesRepositoryImpl> repositories = obtenerRepositoriosParaConsolidacion(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No hay repositorios disponibles para consolidación");
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        try {
            // 2. Recopilar datos DIRECTAMENTE usando QueryStorage sin archivos temporales
            List<Map<String, Object>> todosLosDatos = recopilarDatosConQueryStorageDirecto(
                    queryStorage, repositories, filtros);

            if (todosLosDatos.isEmpty()) {
                return formatoConverter.convertir(Collections.emptyList(),
                        consulta.getFormato() != null ? consulta.getFormato() : "json");
            }

            // 3. Normalizar provincias
            todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

            // 4. Consolidar usando metadata de BD
            List<Map<String, Object>> datosConsolidados = consolidarConMetadataDeBaseDatos(
                    todosLosDatos, filtros, queryStorage);

            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return formatoConverter.convertir(datosConsolidados, formato);

        } catch (Exception e) {
            log.error("Error en consolidación con QueryStorage '{}': {}", queryStorage.getCodigo(), e.getMessage(), e);
            throw new RuntimeException("Error ejecutando consolidación desde BD", e);
        }
    }

    private List<Map<String, Object>> recopilarDatosConQueryStorageDirecto(QueryStorage queryStorage,
                                                                           List<InfraccionesRepositoryImpl> repositories,
                                                                           ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                // Ejecutar SQL DIRECTAMENTE usando el ParametrosProcessor
                QueryResult resultado = parametrosProcessor.procesarQuery(
                        queryStorage.getSqlQuery(), filtros);

                // Ejecutar query SQL directamente en el JdbcTemplate
                List<Map<String, Object>> datosProvider = repo.getNamedParameterJdbcTemplate().queryForList(
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

                // Agregar registro de error en lugar de fallar completamente
                Map<String, Object> registroError = new HashMap<>();
                registroError.put("provincia", provincia);
                registroError.put("error_consolidacion", true);
                registroError.put("mensaje_error", e.getMessage());
                registroError.put("query_codigo", queryStorage.getCodigo());
                registroError.put("timestamp_error", new java.util.Date());

                todosLosDatos.add(registroError);
            }
        }

        log.info("QueryStorage '{}': Total recopilado {} registros de {} provincias",
                queryStorage.getCodigo(), todosLosDatos.size(), repositories.size());

        return todosLosDatos;
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

    /**
     *  Ejecuta query como si fuera un archivo tradicional
     */
    private Object ejecutarQueryComoArchivo(QueryStorage queryStorage, ConsultaQueryDTO consulta) {

        // En lugar de crear archivos temporales, usar directamente el SQL de QueryStorage
        List<InfraccionesRepositoryImpl> repositories = obtenerRepositoriosParaConsolidacion(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");
        }

        // Ejecutar en modo normal (sin consolidación) usando SQL directo
        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();

                        // Procesar parámetros con SQL de BD
                        QueryResult resultado = parametrosProcessor.procesarQuery(
                                queryStorage.getSqlQuery(), consulta.getParametrosFiltros());

                        // Ejecutar
                        List<Map<String, Object>> datos = repo.getNamedParameterJdbcTemplate().queryForList(
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
                .collect(java.util.stream.Collectors.toList());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    // =============== CONSULTAS Y BÚSQUEDAS ===============

    /**
     *  Obtener todas las queries activas
     */
    public List<QueryStorage> obtenerQueriesActivas() {
        return queryRepository.findByActivaTrueOrderByNombreAsc();
    }

    /**
     * 🔍 Buscar queries por texto
     */
    public List<QueryStorage> buscarQueries(String texto) {
        if (texto == null || texto.trim().isEmpty()) {
            return obtenerQueriesActivas();
        }
        return queryRepository.buscarPorTexto(texto);
    }

    /**
     *  Obtener queries por categoría
     */
    public List<QueryStorage> obtenerQueriesPorCategoria(String categoria) {
        return queryRepository.findByCategoriaAndActivaTrueOrderByNombreAsc(categoria);
    }

    /**
     *  Obtener queries consolidables
     */
    public List<QueryStorage> obtenerQueriesConsolidables() {
        return queryRepository.findByEsConsolidableAndActivaTrueOrderByNombreAsc(true);
    }

    /**
     *  Obtener queries más utilizadas
     */
    public List<QueryStorage> obtenerQueriesPopulares(int limite) {
        List<QueryStorage> populares = queryRepository.findByActivaTrueOrderByContadorUsosDescNombreAsc();
        return populares.stream().limit(limite).collect(Collectors.toList());
    }

    /**
     *  Obtener queries por tag
     */
    public List<QueryStorage> obtenerQueriesPorTag(String tag) {
        return queryRepository.findByTag(tag);
    }

    /**
     * 📈 Obtener estadísticas de queries
     */
    public Map<String, Object> obtenerEstadisticas() {
        java.util.Map<String, Object> stats = new java.util.HashMap<>();

        List<QueryStorage> todasActivas = obtenerQueriesActivas();
        List<QueryStorage> consolidables = obtenerQueriesConsolidables();

        stats.put("total_queries_activas", todasActivas.size());
        stats.put("queries_consolidables", consolidables.size());
        stats.put("porcentaje_consolidables",
                todasActivas.size() > 0 ? (double) consolidables.size() / todasActivas.size() * 100 : 0);

        // Estadísticas por categoría
        List<Object[]> porCategoria = queryRepository.contarPorCategoria();
        Map<String, Long> statsCategoria = new java.util.HashMap<>();
        for (Object[] row : porCategoria) {
            statsCategoria.put((String) row[0], (Long) row[1]);
        }
        stats.put("queries_por_categoria", statsCategoria);

        // Query más popular
        List<QueryStorage> populares = obtenerQueriesPopulares(1);
        if (!populares.isEmpty()) {
            QueryStorage masPopular = populares.get(0);

            Map<String, Object> queryMasPopular = new HashMap<>();
            queryMasPopular.put("codigo", masPopular.getCodigo());
            queryMasPopular.put("nombre", masPopular.getNombre());
            queryMasPopular.put("usos", masPopular.getContadorUsos());

            stats.put("query_mas_popular", queryMasPopular);
        }


        return stats;
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private List<InfraccionesRepositoryImpl> obtenerRepositoriosParaConsolidacion(ParametrosFiltrosDTO filtros) {
        // Reutilizar lógica del InfraccionesService existente
        return infraccionesService.determinarRepositories(filtros);
    }

    // =============== MÉTODOS PÚBLICOS ADICIONALES ===============

    /**
     * 🔍 Obtener query por código
     */
    public Optional<QueryStorage> obtenerQueryPorCodigo(String codigo) {
        return queryRepository.findByCodigo(codigo);
    }

    /**
     * ✅ Validar SQL de query
     */
    public boolean validarSqlQuery(String sql) {
        try {
            // Validaciones básicas
            if (sql == null || sql.trim().isEmpty()) {
                return false;
            }

            String sqlUpper = sql.toUpperCase();

            // Debe contener SELECT
            if (!sqlUpper.contains("SELECT")) {
                return false;
            }

            // No debe contener operaciones peligrosas
            String[] operacionesPeligrosas = {"DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE"};
            for (String operacion : operacionesPeligrosas) {
                if (sqlUpper.contains(operacion)) {
                    return false;
                }
            }

            // Debe tener parámetros dinámicos para ser seguro
            if (!sql.contains(":")) {
                log.warn("Query sin parámetros dinámicos: puede ser insegura");
            }

            return true;

        } catch (Exception e) {
            log.error("Error validando SQL: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 🧪 Preview de ejecución de query
     */
    public Map<String, Object> previewQuery(String codigo, ParametrosFiltrosDTO filtros) {
        Map<String, Object> preview = new java.util.HashMap<>();

        try {
            QueryStorage query = queryRepository.findByCodigo(codigo)
                    .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

            preview.put("codigo", query.getCodigo());
            preview.put("nombre", query.getNombre());
            preview.put("consolidable", query.getEsConsolidable());
            preview.put("sql_query", query.getSqlQuery());

            // Analizar si sería consolidación
            boolean seríaConsolidada = filtros != null && filtros.esConsolidado() && query.getEsConsolidable();
            preview.put("ejecutaria_consolidacion", seríaConsolidada);

            if (seríaConsolidada) {
                preview.put("campos_agrupacion", query.getCamposAgrupacionList());
                preview.put("campos_numericos", query.getCamposNumericosList());
            }

            // Estimar provincias que se consultarían
            int provinciaEstimadas = estimarProvinciaObjetivo(filtros);
            preview.put("provincias_objetivo", provinciaEstimadas);

        } catch (Exception e) {
            preview.put("error", e.getMessage());
        }

        return preview;
    }

    private int estimarProvinciaObjetivo(ParametrosFiltrosDTO filtros) {
        if (filtros == null) return 6; // Default: todas las provincias

        if (filtros.esConsolidado()) {
            return 6; // Consolidación siempre usa todas
        }

        if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            return filtros.getBaseDatos().size();
        }

        return 6; // Default
    }

    /**
     * 🔄 Migrar queries de archivos a base de datos
     */
    public int migrarQueriesDesdeArchivos() {
        log.info("Iniciando migración de queries desde archivos a base de datos");

        int migradas = 0;

        try {
            // Obtener todas las queries del enum existente
            org.transito_seguro.enums.Consultas[] consultasArchivos =
                    org.transito_seguro.enums.Consultas.values();

            for (org.transito_seguro.enums.Consultas consulta : consultasArchivos) {
                try {
                    String codigo = consulta.name().toLowerCase().replace("_", "-");

                    // Verificar si ya existe
                    if (queryRepository.findByCodigo(codigo).isPresent()) {
                        log.debug("Query '{}' ya existe en BD, saltando", codigo);
                        continue;
                    }

                    // Cargar SQL del archivo
                    String sql = org.transito_seguro.utils.SqlUtils.cargarQuery(consulta.getArchivoQuery());

                    // Analizar automáticamente
                    AnalisisConsolidacion analisis =
                            queryAnalyzer.analizarParaConsolidacion(sql);


                    // Crear registro en BD
                    QueryStorage queryStorage = QueryStorage.builder()
                            .codigo(codigo)
                            .nombre(consulta.getDescripcion())
                            .descripcion("Migrada automáticamente desde archivo: " + consulta.getArchivoQuery())
                            .sqlQuery(sql)
                            .categoria(determinarCategoriaPorNombre(codigo))
                            .esConsolidable(analisis.isEsConsolidable())
                            .activa(true)
                            .estado(EstadoQuery.ANALIZADA)
                            .creadoPor("SISTEMA_MIGRACION")
                            .build();

                    if (analisis.isEsConsolidable()) {
                        queryStorage.setCamposAgrupacionList(analisis.getCamposAgrupacion());
                        queryStorage.setCamposNumericosList(analisis.getCamposNumericos());
                        queryStorage.setCamposUbicacionList(analisis.getCamposUbicacion());
                        queryStorage.setCamposTiempoList(analisis.getCamposTiempo());
                    }

                    queryRepository.save(queryStorage);
                    migradas++;

                    log.info("Query migrada: {} -> {}", consulta.getArchivoQuery(), codigo);

                } catch (Exception e) {
                    log.error("Error migrando query {}: {}", consulta.getArchivoQuery(), e.getMessage());
                }
            }

            log.info("Migración completada: {} queries migradas", migradas);
            return migradas;

        } catch (Exception e) {
            log.error("Error en migración de queries: {}", e.getMessage(), e);
            return migradas;
        }
    }

    private String determinarCategoriaPorNombre(String codigo) {
        if (codigo.contains("persona")) return "PERSONAS";
        if (codigo.contains("vehiculo")) return "VEHICULOS";
        if (codigo.contains("infraccion")) return "INFRACCIONES";
        if (codigo.contains("radar")) return "RADARES";
        if (codigo.contains("semaforo")) return "SEMAFOROS";
        if (codigo.contains("reporte")) return "REPORTES";
        return "GENERAL";
    }

    /**
     * 🏷️ Gestión de tags
     */
    public List<String> obtenerTodosLosTags() {
        List<QueryStorage> queries = queryRepository.findByActivaTrueOrderByNombreAsc();

        return queries.stream()
                .flatMap(q -> q.getTagsList().stream())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * 📊 Estadísticas de uso por query
     */
    public Map<String, Object> obtenerEstadisticasUso(String codigo) {
        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("codigo", query.getCodigo());
        stats.put("total_usos", query.getContadorUsos());
        stats.put("ultimo_uso", query.getUltimoUso());
        stats.put("fecha_creacion", query.getFechaCreacion());
        stats.put("version_actual", query.getVersion());
        stats.put("consolidable", query.getEsConsolidable());

        return stats;
    }

    /**
     * 🔄 Duplicar query para crear nueva versión
     */
    public QueryStorage duplicarQuery(String codigoOriginal, String nuevoCodigo, String nuevoNombre) {
        QueryStorage original = queryRepository.findByCodigo(codigoOriginal)
                .orElseThrow(() -> new IllegalArgumentException("Query original no encontrada: " + codigoOriginal));

        if (queryRepository.findByCodigo(nuevoCodigo).isPresent()) {
            throw new IllegalArgumentException("Ya existe una query con código: " + nuevoCodigo);
        }

        QueryStorage duplicada = original.toBuilder()
                .id(null) // Nuevo ID
                .codigo(nuevoCodigo)
                .nombre(nuevoNombre != null ? nuevoNombre : original.getNombre() + " (Copia)")
                .version(1)
                .contadorUsos(0L)
                .ultimoUso(null)
                .fechaCreacion(null) // Se establecerá en @PrePersist
                .fechaActualizacion(null)
                .build();

        return queryRepository.save(duplicada);
    }

    /**
     * 📋 Exportar query como DTO
     */
    public QueryStorageDTO exportarQueryComoDTO(String codigo) {
        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        return QueryStorageDTO.builder()
                .codigo(query.getCodigo())
                .nombre(query.getNombre())
                .descripcion(query.getDescripcion())
                .sqlQuery(query.getSqlQuery())
                .categoria(query.getCategoria())
                .esConsolidable(query.getEsConsolidable())
                .camposAgrupacion(query.getCamposAgrupacionList())
                .camposNumericos(query.getCamposNumericosList())
                .camposUbicacion(query.getCamposUbicacionList())
                .camposTiempo(query.getCamposTiempoList())
                .timeoutSegundos(query.getTimeoutSegundos())
                .limiteMaximo(query.getLimiteMaximo())
                .tags(query.getTagsList())
                .activa(query.getActiva())
                .build();
    }

    /**
     * 🧹 Limpiar queries obsoletas automáticamente
     */
    public int limpiarQueriesObsoletas() {
        LocalDateTime fechaCorte = LocalDateTime.now().minusMonths(6);

        List<QueryStorage> paraLimpiar = queryRepository.findAll().stream()
                .filter(q -> q.getEstado() == EstadoQuery.OBSOLETA)
                .filter(q -> q.getUltimoUso() == null || q.getUltimoUso().isBefore(fechaCorte))
                .filter(q -> q.getContadorUsos() == 0)
                .collect(Collectors.toList());

        for (QueryStorage query : paraLimpiar) {
            queryRepository.delete(query);
        }

        log.info("Limpieza automática: {} queries obsoletas eliminadas", paraLimpiar.size());
        return paraLimpiar.size();
    }
}