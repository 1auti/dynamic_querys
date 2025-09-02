package org.transito_seguro.repository.impl;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.Consultas;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.utils.SqlUtils;
import java.util.List;
import java.util.Map;

public class InfraccionesRepositoryImpl implements InfraccionesRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    /**
     * -- GETTER --
     *  Obtener información de la provincia asociada a este repository
     */
    @Getter
    private final String provincia;

    @Autowired
    private ParametrosProcessor parametrosProcessor;

    public InfraccionesRepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate, String provincia) {
        this.jdbcTemplate = jdbcTemplate;
        this.provincia = provincia;
    }

    @Override
    public List<Map<String, Object>> consultarPersonasJuridicas(ParametrosFiltrosDTO filtro) {
        String queryBase = SqlUtils.cargarQuery(Consultas.PERSONAS_JURUDICAS.getArchivoQuery());

        // Procesar la query con filtros dinámicos
        ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(queryBase, filtro);

        return jdbcTemplate.queryForList(resultado.getQueryModificada(), resultado.getParametros());
    }

    /**
     * Método genérico para ejecutar cualquier query con filtros dinámicos
     */
    public List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros) {
        String queryBase = SqlUtils.cargarQuery(nombreQuery);

        // Procesar la query con filtros dinámicos
        ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(queryBase, filtros);

        return jdbcTemplate.queryForList(resultado.getQueryModificada(), resultado.getParametros());
    }

    /**
     * Método para consultar infracciones generales con filtros dinámicos
     */
    public List<Map<String, Object>> consultarInfraccionesGenerales(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_GENERAL.getArchivoQuery(), filtros);
    }

    /**
     * Método para consultar infracciones por equipos con filtros dinámicos
     */
    public List<Map<String, Object>> consultarInfraccionesPorEquipos(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_POR_EQUIPO.getArchivoQuery(),filtros);
    }

    /**
     * Método para consultar vehículos por municipio con filtros dinámicos
     */
    public List<Map<String, Object>> consultarVehiculosPorMunicipio(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.VEHICULOS_POR_MUNICIPIO.getArchivoQuery(), filtros);
    }

    /**
     * Método para consultar infracciones de radar fijo con filtros dinámicos
     */
    public List<Map<String, Object>> consultarRadarFijoPorEquipo(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.RADAR_FIJO_POR_EQUIPO.getArchivoQuery(), filtros);
    }

    /**
     * Método para consultar infracciones de semáforo con filtros dinámicos
     */
    public List<Map<String, Object>> consultarSemaforoPorEquipo(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.SEMAFORO_POR_EQUIPO.getArchivoQuery(), filtros);
    }

    /**
     * Método para consultar infracciones sin email con filtros dinámicos
     */
    public List<Map<String, Object>> consultarSinEmailPorMunicipio(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.SIN_EMAIL_POR_MUNICIPIO.getArchivoQuery(), filtros);
    }

    /**
     * Método para verificar imágenes de radar con filtros dinámicos
     */
    public List<Map<String, Object>> verificarImagenesRadar(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.VERIFICAR_IMAGENES_RADAR.getArchivoQuery(), filtros);
    }

    /**
     * Método para consultar reporte detallado con filtros dinámicos
     */
    public List<Map<String, Object>> consultarReporteDetallado(ParametrosFiltrosDTO filtros) {
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_DETALLADO.getArchivoQuery(), filtros);
    }

}