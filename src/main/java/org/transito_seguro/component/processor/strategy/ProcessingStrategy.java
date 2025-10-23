package org.transito_seguro.component.processor.strategy;

import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.List;

/**
 * Estrategia de procesamiento de datos por provincia.
 *
 * Define el contrato para las diferentes estrategias:
 * - PARALELO: Todas las provincias simultáneamente
 * - SECUENCIAL: Una provincia a la vez
 * - HÍBRIDO: Grupos de provincias en paralelo
 */
public interface ProcessingStrategy {

    /**
     * Ejecuta el procesamiento según la estrategia implementada.
     *
     * @param repositories Lista de repositorios (uno por provincia)
     * @param filtros Filtros aplicados a las queries
     * @param nombreQuery Código de la query a ejecutar
     * @param contexto Contexto para acumular resultados
     * @param queryStorage Metadata de la query
     */
    void ejecutar(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage
    );

    /**
     * Nombre descriptivo de la estrategia para logging.
     *
     * @return Nombre de la estrategia (ej: "PARALELO", "SECUENCIAL")
     */
    String getNombre();
}