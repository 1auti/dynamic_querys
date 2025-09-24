package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

/**
 * Servicio Registry que gestiona el análisis y metadata de queries almacenadas en base de datos.
 *
 * Este servicio proporciona:
 * - Análisis de consolidación basado en metadata almacenada
 * - Búsqueda inteligente de queries por múltiples criterios
 * - Re-análisis de queries cuando se modifica su SQL
 * - Estadísticas y métricas del registry
 * - Funciones de mantenimiento y limpieza
 *
 * @author Sistema Tránsito Seguro
 * @version 2.0 - Sin migración automática
 */
@Slf4j
@Service
@Transactional
public class QueryRegistryService {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private QueryStorageRepository queryRepository;

    @Autowired
    private QueryAnalyzer queryAnalyzer;

    // =============== API PRINCIPAL DE ANÁLISIS ===============

    /**
     * Obtiene análisis de consolidación para una query específica.
     *
     * Prioriza metadata almacenada en BD sobre análisis dinámico para mejor performance.
     * Solo realiza análisis dinámico como fallback si no hay metadata disponible.
     *
     * @param nombreQuery Código único de la query en base de datos
     * @return QueryAnalyzer.AnalisisConsolidacion Análisis con metadata de consolidación
     */
    public QueryAnalyzer.AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        log.debug("Obteniendo análisis de consolidación para: {}", nombreQuery);

        // 1. Buscar metadata en BD (estrategia prioritaria)
        String codigoQuery = normalizarCodigoQuery(nombreQuery);
        Optional<QueryStorage> queryStorage = queryRepository.findByCodigo(codigoQuery);

        if (queryStorage.isPresent() && queryStorage.get().estaLista()) {
            QueryStorage query = queryStorage.get();
            log.debug("Análisis encontrado en BD para query: {}", codigoQuery);

            // Registrar acceso para estadísticas
            registrarAccesoQuery(query);

            return crearAnalisisDesdeQueryStorage(query);
        }

        // 2. Query no encontrada en BD
        log.warn("Query '{}' no encontrada en base de datos. Códigos disponibles: {}",
                codigoQuery, obtenerCodigosDisponibles());

        return crearAnalisisVacio();
    }

    /**
     * Busca una query por múltiples criterios de identificación.
     *
     * @param identificador Código, nombre o identificador de la query
     * @return Optional<QueryStorage> Query encontrada o empty si no existe
     */
    public Optional<QueryStorage> buscarQuery(String identificador) {
        log.debug("Buscando query con identificador: {}", identificador);

        // 1. Búsqueda exacta por código
        Optional<QueryStorage> resultado = queryRepository.findByCodigo(identificador);
        if (resultado.isPresent()) {
            log.debug("Query encontrada por código exacto: {}", identificador);
            return resultado;
        }

        // 2. Búsqueda con normalización
        String codigoNormalizado = normalizarCodigoQuery(identificador);
        resultado = queryRepository.findByCodigo(codigoNormalizado);
        if (resultado.isPresent()) {
            log.debug("Query encontrada por código normalizado: {} -> {}", identificador, codigoNormalizado);
            return resultado;
        }

        log.debug("Query no encontrada: {}", identificador);
        return Optional.empty();
    }

    /**
     * Re-analiza una query existente actualizando su metadata de consolidación.
     *
     * @param codigo Código único de la query
     * @return QueryStorage Query con metadata actualizada
     * @throws IllegalArgumentException si la query no existe
     * @throws RuntimeException si hay errores durante el re-análisis
     */
    public QueryStorage reAnalizarQuery(String codigo) {
        log.info("Iniciando re-análisis de query: {}", codigo);

        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        try {
            // Realizar análisis del SQL actual
            QueryAnalyzer.AnalisisConsolidacion analisis =
                    queryAnalyzer.analizarParaConsolidacion(query.getSqlQuery());

            // Actualizar metadata de consolidación
            actualizarMetadataConsolidacion(query, analisis);

            // Marcar como analizada y incrementar versión
            query.setEstado(EstadoQuery.ANALIZADA);
            query.setVersion(query.getVersion() + 1);

            QueryStorage actualizada = queryRepository.save(query);

            log.info("Query re-analizada exitosamente: {} (Consolidable: {})",
                    codigo, actualizada.getEsConsolidable());

            return actualizada;

        } catch (Exception e) {
            // Marcar query como error y guardar
            query.setEstado(EstadoQuery.ERROR);
            queryRepository.save(query);

            log.error("Error re-analizando query '{}': {}", codigo, e.getMessage(), e);
            throw new RuntimeException("Error en re-análisis de query: " + codigo, e);
        }
    }

    // =============== ESTADÍSTICAS Y MÉTRICAS ===============

    /**
     * Obtiene estadísticas completas del registry.
     *
     * @return Map<String, Object> Estadísticas detalladas del sistema
     */
    public Map<String, Object> obtenerEstadisticasRegistry() {
        log.debug("Generando estadísticas del registry");

        Map<String, Object> stats = new HashMap<>();

        List<QueryStorage> todasActivas = queryRepository.findByActivaTrueOrderByNombreAsc();
        List<QueryStorage> consolidables = queryRepository.findByEsConsolidableAndActivaTrueOrderByNombreAsc(true);

        // Estadísticas básicas
        stats.put("total_queries_activas", todasActivas.size());
        stats.put("queries_consolidables", consolidables.size());
        stats.put("porcentaje_consolidables", calcularPorcentaje(consolidables.size(), todasActivas.size()));

        // Estadísticas por estado
        stats.put("queries_por_estado", obtenerEstadisticasPorEstado());

        // Estadísticas por categoría
        stats.put("queries_por_categoria", obtenerEstadisticasPorCategoria());

        // Query más utilizada
        stats.put("query_mas_utilizada", obtenerQueryMasUtilizada());

        log.debug("Estadísticas generadas: {} queries activas, {} consolidables",
                todasActivas.size(), consolidables.size());

        return stats;
    }

    /**
     * Obtiene lista de códigos de queries disponibles.
     *
     * @return List<String> Lista de códigos de queries activas
     */
    public List<String> obtenerCodigosDisponibles() {
        return queryRepository.findByActivaTrueOrderByNombreAsc()
                .stream()
                .map(QueryStorage::getCodigo)
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Verifica si una query específica puede ser consolidada.
     *
     * @param nombreQuery Código de la query
     * @return boolean true si la query es consolidable
     */
    public boolean esQueryConsolidable(String nombreQuery) {
        try {
            QueryAnalyzer.AnalisisConsolidacion analisis = obtenerAnalisisConsolidacion(nombreQuery);
            return analisis.isEsConsolidado();
        } catch (Exception e) {
            log.error("Error verificando consolidación para query '{}': {}", nombreQuery, e.getMessage());
            return false;
        }
    }

    // =============== MANTENIMIENTO Y LIMPIEZA ===============

    /**
     * Elimina queries inactivas que no tienen uso registrado.
     *
     * @return int Número de queries eliminadas
     */
    public int limpiarQueriesInactivas() {
        log.info("Iniciando limpieza de queries inactivas");

        List<QueryStorage> queriesParaEliminar = queryRepository.findAll().stream()
                .filter(this::debeSerEliminada)
                .collect(java.util.stream.Collectors.toList());

        if (!queriesParaEliminar.isEmpty()) {
            queryRepository.deleteAll(queriesParaEliminar);
            log.info("Limpieza completada: {} queries eliminadas", queriesParaEliminar.size());
        } else {
            log.info("No hay queries para eliminar");
        }

        return queriesParaEliminar.size();
    }

    /**
     * Re-analiza todas las queries activas para actualizar su metadata.
     *
     * @return int Número de queries re-analizadas exitosamente
     */
    public int reAnalizarTodasLasQueries() {
        log.info("Iniciando re-análisis de todas las queries activas");

        List<QueryStorage> queriesActivas = queryRepository.findByActivaTrueOrderByNombreAsc();
        int exitosas = 0;
        int errores = 0;

        for (QueryStorage query : queriesActivas) {
            try {
                reAnalizarQuery(query.getCodigo());
                exitosas++;
            } catch (Exception e) {
                errores++;
                log.error("Error re-analizando query '{}': {}", query.getCodigo(), e.getMessage());
            }
        }

        log.info("Re-análisis completado: {} exitosas, {} con errores", exitosas, errores);
        return exitosas;
    }

    // =============== MÉTODOS PRIVADOS DE UTILIDAD ===============

    /**
     * Crea análisis de consolidación desde QueryStorage almacenado.
     *
     * @param query QueryStorage con metadata
     * @return QueryAnalyzer.AnalisisConsolidacion Análisis construido
     */
    private QueryAnalyzer.AnalisisConsolidacion crearAnalisisDesdeQueryStorage(QueryStorage query) {
        return new QueryAnalyzer.AnalisisConsolidacion(
                query.getCamposAgrupacionList() != null ? query.getCamposAgrupacionList() : java.util.Collections.emptyList(),
                query.getCamposNumericosList() != null ? query.getCamposNumericosList() : java.util.Collections.emptyList(),
                query.getCamposTiempoList() != null ? query.getCamposTiempoList() : java.util.Collections.emptyList(),
                query.getCamposUbicacionList() != null ? query.getCamposUbicacionList() : java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(), // tipoPorCampo - no almacenado actualmente
                query.getEsConsolidable() != null ? query.getEsConsolidable() : false
        );
    }

    /**
     * Crea análisis vacío para casos de error o query no encontrada.
     *
     * @return QueryAnalyzer.AnalisisConsolidacion Análisis vacío
     */
    private QueryAnalyzer.AnalisisConsolidacion crearAnalisisVacio() {
        return new QueryAnalyzer.AnalisisConsolidacion(
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(),
                false
        );
    }

    /**
     * Normaliza código de query para búsqueda consistente.
     *
     * @param codigo Código original
     * @return String Código normalizado
     */
    private String normalizarCodigoQuery(String codigo) {
        return codigo
                .replace(".sql", "")
                .replace("_", "-")
                .toLowerCase()
                .trim();
    }

    /**
     * Registra acceso a una query para estadísticas.
     *
     * @param query Query accedida
     */
    private void registrarAccesoQuery(QueryStorage query) {
        try {
            query.registrarUso();
            queryRepository.save(query);
        } catch (Exception e) {
            log.warn("Error registrando acceso a query '{}': {}", query.getCodigo(), e.getMessage());
        }
    }

    /**
     * Actualiza metadata de consolidación en una query.
     *
     * @param query Query a actualizar
     * @param analisis Análisis con nueva metadata
     */
    private void actualizarMetadataConsolidacion(QueryStorage query,
                                                 QueryAnalyzer.AnalisisConsolidacion analisis) {
        query.setEsConsolidable(analisis.isEsConsolidado());

        if (analisis.isEsConsolidado()) {
            query.setCamposAgrupacionList(analisis.getCamposAgrupacion());
            query.setCamposNumericosList(analisis.getCamposNumericos());
            query.setCamposUbicacionList(analisis.getCamposUbicacion());
            query.setCamposTiempoList(analisis.getCamposTiempo());
        } else {
            // Limpiar metadata si ya no es consolidable
            query.setCamposAgrupacionList(null);
            query.setCamposNumericosList(null);
            query.setCamposUbicacionList(null);
            query.setCamposTiempoList(null);
        }
    }

    /**
     * Determina si una query debe ser eliminada durante limpieza.
     *
     * @param query Query a evaluar
     * @return boolean true si debe ser eliminada
     */
    private boolean debeSerEliminada(QueryStorage query) {
        return (!query.getActiva() || query.getEstado() == EstadoQuery.OBSOLETA)
                && query.getContadorUsos() == 0;
    }

    /**
     * Calcula porcentaje con manejo de división por cero.
     *
     * @param numerador Numerador
     * @param denominador Denominador
     * @return double Porcentaje calculado
     */
    private double calcularPorcentaje(int numerador, int denominador) {
        return denominador > 0 ? (double) numerador / denominador * 100 : 0.0;
    }

    /**
     * Obtiene estadísticas de queries por estado.
     *
     * @return Map<String, Long> Conteo por estado
     */
    private Map<String, Long> obtenerEstadisticasPorEstado() {
        Map<String, Long> estadisticas = new HashMap<>();

        for (EstadoQuery estado : EstadoQuery.values()) {
            long count = queryRepository.findByEstadoAndActivaTrueOrderByNombreAsc(estado).size();
            estadisticas.put(estado.name(), count);
        }

        return estadisticas;
    }

    /**
     * Obtiene estadísticas de queries por categoría.
     *
     * @return Map<String, Long> Conteo por categoría
     */
    private Map<String, Long> obtenerEstadisticasPorCategoria() {
        Map<String, Long> estadisticas = new HashMap<>();

        queryRepository.findByActivaTrueOrderByNombreAsc()
                .forEach(query -> {
                    String categoria = query.getCategoria() != null ? query.getCategoria() : "SIN_CATEGORIA";
                    estadisticas.merge(categoria, 1L, Long::sum);
                });

        return estadisticas;
    }

    /**
     * Obtiene información de la query más utilizada.
     *
     * @return Map<String, Object> Información de la query más popular
     */
    private Map<String, Object> obtenerQueryMasUtilizada() {
        List<QueryStorage> queries = queryRepository.findByActivaTrueOrderByContadorUsosDescNombreAsc();

        if (queries.isEmpty()) {
            return java.util.Collections.emptyMap();
        }

        QueryStorage masUtilizada = queries.get(0);
        Map<String, Object> info = new HashMap<>();
        info.put("codigo", masUtilizada.getCodigo());
        info.put("nombre", masUtilizada.getNombre());
        info.put("usos", masUtilizada.getContadorUsos());
        info.put("consolidable", masUtilizada.getEsConsolidable());

        return info;
    }
}