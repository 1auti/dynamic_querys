package org.transito_seguro.repository;

import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.List;
import java.util.Map;

/**
 * Interfaz para el repositorio de consultas de infracciones de tránsito.
 * Define todos los métodos de consulta disponibles para diferentes tipos de reportes.
 */
public interface InfraccionesRepository {

    /**
     * Método genérico para ejecutar cualquier query con filtros dinámicos
     * @param nombreQuery Nombre del archivo de query SQL
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros);

    /**
     * Obtiene el nombre de la provincia asociada a este repositorio
     * @return Nombre de la provincia
     */
    String getProvincia();

}