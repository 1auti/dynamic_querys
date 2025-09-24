package org.transito_seguro.repository.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.Consultas;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.utils.SqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implementación del repositorio de infracciones para una provincia específica.
 * Maneja la ejecución de consultas SQL dinámicas con filtros parametrizados.
 * NUEVO: Soporte para queries desde BD y archivos
 */
@Slf4j
public class InfraccionesRepositoryImpl implements InfraccionesRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Getter
    private final String provincia;

    private final ParametrosProcessor parametrosProcessor;

    // NUEVO: Repositorio para queries en BD
    private QueryStorageRepository queryStorageRepository;

    /**
     * Constructor que recibe todas las dependencias por parámetro
     */
    public InfraccionesRepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate,
                                      String provincia,
                                      ParametrosProcessor parametrosProcessor) {
        this.jdbcTemplate = jdbcTemplate;
        this.provincia = provincia;
        this.parametrosProcessor = parametrosProcessor;

        log.debug("Inicializado InfraccionesRepositoryImpl para provincia: {}", provincia);
    }

    /**
     * NUEVO: Setter para inyectar QueryStorageRepository después de la construcción
     */
    public void setQueryStorageRepository(QueryStorageRepository queryStorageRepository) {
        this.queryStorageRepository = queryStorageRepository;
    }

    @Override
    public List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros) {
        log.debug("Ejecutando query '{}' en provincia: {}", nombreQuery, provincia);

        try {
            // 1. NUEVO: Intentar cargar desde BD primero
            String querySQL = cargarQueryDesdeBaseDatosOArchivo(nombreQuery);

            // 2. Procesar la query con filtros dinámicos
            ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(querySQL, filtros);

            // 3. Ejecutar
            List<Map<String, Object>> resultados = jdbcTemplate.queryForList(
                    resultado.getQueryModificada(),
                    resultado.getParametros()
            );

            log.debug("Query '{}' completada. Resultados: {} registros", nombreQuery, resultados.size());
            return resultados;

        } catch (Exception e) {
            log.error("Error ejecutando query '{}' en provincia {}: {}",
                    nombreQuery, provincia, e.getMessage(), e);
            throw new RuntimeException("Error ejecutando query: " + nombreQuery, e);
        }
    }

    /**
     * NUEVO: Método híbrido que carga query desde BD o archivo
     */
    private String cargarQueryDesdeBaseDatosOArchivo(String nombreQuery) {

        // 1. Intentar desde BD primero
        if (queryStorageRepository != null) {
            try {
                Optional<QueryStorage> queryStorage = queryStorageRepository.findByCodigo(nombreQuery);

                if (queryStorage.isPresent() && queryStorage.get().estaLista()) {
                    log.debug("✅ Query '{}' cargada desde BD", nombreQuery);
                    return queryStorage.get().getSqlQuery();
                }

                log.debug("❌ Query '{}' no encontrada en BD", nombreQuery);

            } catch (Exception e) {
                log.warn("Error accediendo BD para query '{}': {}", nombreQuery, e.getMessage());
            }
        }

        // 2. Fallback: cargar desde archivo
        try {
            String querySQL = SqlUtils.cargarQuery(nombreQuery);
            log.debug("📁 Query '{}' cargada desde archivo", nombreQuery);
            return querySQL;

        } catch (Exception e) {
            log.error("❌ Query '{}' no encontrada ni en BD ni en archivos", nombreQuery);
            throw new RuntimeException("Query no encontrada: " + nombreQuery +
                    ". No existe en BD ni en archivos.", e);
        }
    }

    // =============== MÉTODOS EXISTENTES SIN CAMBIOS ===============

    @Override
    public List<Map<String, Object>> consultarPersonasJuridicas(ParametrosFiltrosDTO filtro) {
        log.debug("Consultando personas jurídicas en provincia: {}", provincia);
        return ejecutarQueryConFiltros("consultar_personas_juridicas.sql", filtro);
    }

    @Override
    public List<Map<String, Object>> consultarInfraccionesGenerales(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando infracciones generales en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_GENERAL.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarInfraccionesPorEquipos(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando infracciones por equipos en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_POR_EQUIPOS.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarVehiculosPorMunicipio(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando vehículos por municipio en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.VEHICULOS_POR_MUNICIPIO.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarRadarFijoPorEquipo(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando radar fijo por equipo en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.RADAR_FIJO_POR_EQUIPO.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarSemaforoPorEquipo(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando semáforo por equipo en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.SEMAFORO_POR_EQUIPO.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarSinEmailPorMunicipio(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando infracciones sin email por municipio en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.SIN_EMAIL_POR_MUNICIPIO.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> verificarImagenesRadar(ParametrosFiltrosDTO filtros) {
        log.debug("Verificando imágenes de radar en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.VERIFICAR_IMAGENES_RADAR.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarReporteDetallado(ParametrosFiltrosDTO filtros) {
        log.debug("Consultando reporte detallado en provincia: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_DETALLADO.getArchivoQuery(), filtros);
    }

    @Override
    public List<Map<String, Object>> consultarCantidadInfraccionesEstado(ParametrosFiltrosDTO filtrosDTO) {
        log.debug("Consultar reporte de cantidad de infracciones por estado: {}", provincia);
        return ejecutarQueryConFiltros(Consultas.INFRACCIONES_POR_ESTADO.getArchivoQuery(), filtrosDTO);
    }

    /**
     * Método utilitario para validar la conectividad del repositorio
     */
    public boolean validarConectividad() {
        try {
            jdbcTemplate.queryForObject("SELECT 1 as test", new HashMap<String, Object>(), Integer.class);
            log.debug("Conectividad validada exitosamente para provincia: {}", provincia);
            return true;
        } catch (Exception e) {
            log.warn("Error validando conectividad para provincia {}: {}", provincia, e.getMessage());
            return false;
        }
    }

    /**
     * Método utilitario para obtener información del datasource
     */
    public Map<String, Object> obtenerInfoDatasource() {
        try {
            Map<String, Object> info = new HashMap<>();
            info.put("provincia", provincia);
            info.put("conexion_activa", validarConectividad());
            info.put("tipo_driver", jdbcTemplate.getJdbcTemplate().getDataSource().getConnection().getMetaData().getDriverName());
            return info;
        } catch (Exception e) {
            log.warn("Error obteniendo info del datasource para provincia {}: {}", provincia, e.getMessage());
            Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("provincia", provincia);
            errorInfo.put("conexion_activa", false);
            errorInfo.put("error", e.getMessage());
            return errorInfo;
        }
    }

    /**
     * Método utilitario para estadísticas de uso
     */
    public Map<String, Object> obtenerEstadisticas() {
        Map<String, Object> estadisticas = new HashMap<>();
        estadisticas.put("provincia", provincia);
        estadisticas.put("queries_disponibles", Consultas.values().length);
        estadisticas.put("conexion_activa", validarConectividad());
        return estadisticas;
    }

    @Override
    public String toString() {
        return String.format("InfraccionesRepositoryImpl{provincia='%s'}", provincia);
    }

    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
        return jdbcTemplate;
    }
}