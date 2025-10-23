package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.analyzer.QueryAnalyzer;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;

import java.util.List;
import java.util.Optional;

import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;

/**
 * Servicio Registry que gestiona el análisis y metadata de queries almacenadas en base de datos.
 * Este servicio proporciona:
 * - Análisis de consolidación basado en metadata almacenada
 * - Búsqueda inteligente de queries por múltiples criterios
 * - Re-análisis de queries cuando se modifica su SQL
 * - Estadísticas y métricas del registry
 * - Funciones de mantenimiento y limpieza
 *
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
    public AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
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



    // =============== MÉTODOS PRIVADOS DE UTILIDAD ===============

    /**
     * Crea análisis de consolidación desde QueryStorage almacenado.
     *
     * @param query QueryStorage con metadata
     * @return QueryAnalyzer.AnalisisConsolidacion Análisis construido
     */
    private AnalisisConsolidacion crearAnalisisDesdeQueryStorage(QueryStorage query) {
        return new AnalisisConsolidacion(
                query.getCamposAgrupacionList() != null ? query.getCamposAgrupacionList() : java.util.Collections.emptyList(),
                query.getCamposNumericosList() != null ? query.getCamposNumericosList() : java.util.Collections.emptyList(),
                query.getCamposTiempoList() != null ? query.getCamposTiempoList() : java.util.Collections.emptyList(),
                query.getCamposUbicacionList() != null ? query.getCamposUbicacionList() : java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(), // tipoPorCampo - no almacenado actualmente
                query.getEsConsolidable() != null ? query.getEsConsolidable() : false, null, null, null, null
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




}