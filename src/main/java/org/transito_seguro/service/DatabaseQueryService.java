package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.QueryStorageDTO;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import javax.xml.bind.ValidationException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 🗄️ Servicio para gestionar queries almacenadas en base de datos
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
    private ConsolidacionService consolidacionService;

    @Autowired
    private InfraccionesService infraccionesService;



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

        // Analizar query automáticamente
        QueryAnalyzer.AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(dto.getSqlQuery());

        // Crear entidad
        QueryStorage query = QueryStorage.builder()
                .codigo(dto.getCodigo())
                .nombre(dto.getNombre())
                .descripcion(dto.getDescripcion())
                .sqlQuery(dto.getSqlQuery())
                .categoria(dto.getCategoria() != null ? dto.getCategoria() : "GENERAL")
                .esConsolidable(analisis.isEsConsolidado())
                .timeoutSegundos(dto.getTimeoutSegundos())
                .limiteMaximo(dto.getLimiteMaximo())
                .creadoPor(dto.getCreadoPor())
                .activa(dto.getActiva())
                .estado(EstadoQuery.ANALIZADA)
                .build();

        // Aplicar análisis automático o manual
        if (analisis.isEsConsolidado()) {
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

        QueryStorage guardada = queryRepository.save(query);
        log.info("Query '{}' guardada exitosamente - ID: {}, Consolidable: {}",
                guardada.getCodigo(), guardada.getId(), guardada.getEsConsolidable());

        return guardada;
    }

    /**
     * ✏️ Actualizar query existente
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
            QueryAnalyzer.AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(dto.getSqlQuery());
            query.setEsConsolidable(analisis.isEsConsolidado());

            if (analisis.isEsConsolidado()) {
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
     * 🗑️ Eliminar query (soft delete)
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
     * 🚀 MÉTODO PRINCIPAL: Ejecutar query desde base de datos
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
     * 🎯 Ejecutar query con consolidación dinámica
     */
    private Object ejecutarQueryConsolidada(QueryStorage queryStorage, ConsultaQueryDTO consulta) {
        log.info("Ejecutando query CONSOLIDADA desde BD: {}", queryStorage.getCodigo());

        // Usar el sistema de consolidación existente pero con SQL de BD
        return ejecutarConsolidacionConQueryStorage(queryStorage, consulta);
    }

    /**
     * 📊 Ejecutar query normal (sin consolidación)
     */
    private Object ejecutarQueryNormal(QueryStorage queryStorage, ConsultaQueryDTO consulta) throws ValidationException {
        log.info("Ejecutando query NORMAL desde BD: {}", queryStorage.getCodigo());

        // Ejecutar usando el servicio existente simulando un archivo
        return ejecutarQueryComoArchivo(queryStorage, consulta);
    }

    // =============== INTEGRACIÓN CON SISTEMA EXISTENTE ===============

    /**
     * 🔗 Ejecuta consolidación usando QueryStorage en lugar de archivo
     */
    private Object ejecutarConsolidacionConQueryStorage(QueryStorage queryStorage, ConsultaQueryDTO consulta) {

        // Simular estructura que espera ConsolidacionService
        List<InfraccionesRepositoryImpl> repositories = obtenerRepositoriosParaConsolidacion(consulta.getParametrosFiltros());

        // Crear un "nombreQuery" temporal para usar con el sistema existente
        String nombreQueryTemporal = crearArchivoTemporalParaQuery(queryStorage);

        try {
            // Usar ConsolidacionService existente
            List<Map<String, Object>> datosConsolidados = consolidacionService.consolidarDatos(
                    repositories, nombreQueryTemporal, consulta.getParametrosFiltros());

            // Formatear respuesta
            String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
            return consolidacionService.generarRespuestaConsolidadaOptima(datosConsolidados, formato);

        } finally {
            // Limpiar archivo temporal
            limpiarArchivoTemporal(nombreQueryTemporal);
        }
    }

    /**
     * 📄 Ejecuta query como si fuera un archivo tradicional
     */
    private Object ejecutarQueryComoArchivo(QueryStorage queryStorage, ConsultaQueryDTO consulta) throws ValidationException {

        // Crear archivo temporal
        String nombreQueryTemporal = crearArchivoTemporalParaQuery(queryStorage);

        try {
            // Usar InfraccionesService existente
            return infraccionesService.ejecutarConsultaPorTipo(
                    nombreQueryTemporal.replace(".sql", ""), consulta);

        } finally {
            // Limpiar archivo temporal
            limpiarArchivoTemporal(nombreQueryTemporal);
        }
    }

    // =============== CONSULTAS Y BÚSQUEDAS ===============

    /**
     * 📋 Obtener todas las queries activas
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
     * 📊 Obtener queries por categoría
     */
    public List<QueryStorage> obtenerQueriesPorCategoria(String categoria) {
        return queryRepository.findByCategoriaAndActivaTrueOrderByNombreAsc(categoria);
    }

    /**
     * 🎯 Obtener queries consolidables
     */
    public List<QueryStorage> obtenerQueriesConsolidables() {
        return queryRepository.findByEsConsolidableAndActivaTrueOrderByNombreAsc(true);
    }

    /**
     * 🔥 Obtener queries más utilizadas
     */
    public List<QueryStorage> obtenerQueriesPopulares(int limite) {
        List<QueryStorage> populares = queryRepository.findByActivaTrueOrderByContadorUsosDescNombreAsc();
        return populares.stream().limit(limite).collect(Collectors.toList());
    }

    /**
     * 🏷️ Obtener queries por tag
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
        if (filtros != null && filtros.esConsolidado()) {
            // Para consolidación, usar todas las provincias
            return infraccionesService.determinarRepositories(filtros);
        }
        return Collections.emptyList();
    }

    /**
     * 📁 Crea archivo temporal para integrar con sistema existente
     */
    private String crearArchivoTemporalParaQuery(QueryStorage queryStorage) {
        String nombreTemporal = "temp_" + queryStorage.getCodigo().replace("-", "_") + ".sql";

        try {
            // En un sistema real, escribirías el SQL a un archivo temporal
            // Por ahora, almacenar en cache o usar estrategia diferente
            java.io.File tempFile = java.io.File.createTempFile("query_", ".sql");
            java.nio.file.Files.write(tempFile.toPath(), queryStorage.getSqlQuery().getBytes());

            log.debug("Archivo temporal creado: {} para query: {}", tempFile.getPath(), queryStorage.getCodigo());
            return tempFile.getName();

        } catch (Exception e) {
            log.error("Error creando archivo temporal para query {}: {}", queryStorage.getCodigo(), e.getMessage());
            throw new RuntimeException("Error creando archivo temporal", e);
        }
    }

    private void limpiarArchivoTemporal(String nombreArchivo) {
        try {
            // Limpiar archivo temporal creado
            java.io.File tempFile = new java.io.File(System.getProperty("java.io.tmpdir"), nombreArchivo);
            if (tempFile.exists()) {
                tempFile.delete();
                log.debug("Archivo temporal eliminado: {}", nombreArchivo);
            }
        } catch (Exception e) {
            log.warn("Error eliminando archivo temporal {}: {}", nombreArchivo, e.getMessage());
        }
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
                    QueryAnalyzer.AnalisisConsolidacion analisis =
                            queryAnalyzer.analizarParaConsolidacion(sql);

                    // Crear registro en BD
                    QueryStorage queryStorage = QueryStorage.builder()
                            .codigo(codigo)
                            .nombre(consulta.getDescripcion())
                            .descripcion("Migrada automáticamente desde archivo: " + consulta.getArchivoQuery())
                            .sqlQuery(sql)
                            .categoria(determinarCategoriaPorNombre(codigo))
                            .esConsolidable(analisis.isEsConsolidado())
                            .activa(true)
                            .estado(EstadoQuery.ANALIZADA)
                            .creadoPor("SISTEMA_MIGRACION")
                            .build();

                    if (analisis.isEsConsolidado()) {
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