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
     * Consulta personas jurídicas con filtros dinámicos
     * @param filtro Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarPersonasJuridicas(ParametrosFiltrosDTO filtro);

    /**
     * Método genérico para ejecutar cualquier query con filtros dinámicos
     * @param nombreQuery Nombre del archivo de query SQL
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros);

    /**
     * Consulta infracciones generales con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarInfraccionesGenerales(ParametrosFiltrosDTO filtros);

    /**
     * Consulta infracciones por equipos con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarInfraccionesPorEquipos(ParametrosFiltrosDTO filtros);

    /**
     * Consulta vehículos por municipio con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarVehiculosPorMunicipio(ParametrosFiltrosDTO filtros);

    /**
     * Consulta infracciones de radar fijo por equipo con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarRadarFijoPorEquipo(ParametrosFiltrosDTO filtros);

    /**
     * Consulta infracciones de semáforo por equipo con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarSemaforoPorEquipo(ParametrosFiltrosDTO filtros);

    /**
     * Consulta infracciones sin email por municipio con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarSinEmailPorMunicipio(ParametrosFiltrosDTO filtros);

    /**
     * Verifica imágenes de radar con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> verificarImagenesRadar(ParametrosFiltrosDTO filtros);

    /**
     * Consulta reporte detallado con filtros dinámicos
     * @param filtros Parámetros de filtrado
     * @return Lista de mapas con los resultados
     */
    List<Map<String, Object>> consultarReporteDetallado(ParametrosFiltrosDTO filtros);

    List<Map<String,Object>> consultarCantidadInfraccionesEstado(ParametrosFiltrosDTO filtrosDTO);


    /**
     * Obtiene el nombre de la provincia asociada a este repositorio
     * @return Nombre de la provincia
     */
    String getProvincia();
}