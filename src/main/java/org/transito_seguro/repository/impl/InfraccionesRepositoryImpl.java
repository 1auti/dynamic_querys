package org.transito_seguro.repository.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.component.DynamicBuilderQuery;
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

    private  DynamicBuilderQuery builderQuery;

    /**
     *   Setter para inyectar QueryStorageRepository después de la construcción
     */

    @Setter
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

    @Override
    public List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros) {
        log.debug("Ejecutando query '{}' en provincia: {}", nombreQuery, provincia);

        try {
            // 1. Intentar cargar desde BD primero
            String querySQL = cargarQueryDesdeBaseDatosOArchivo(nombreQuery);

            // 3. Procesar la query con filtros dinámicos
            ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(querySQL, filtros);

            // 4. Ejecutar
            List<Map<String, Object>> resultados = jdbcTemplate.queryForList(
                    resultado.getQueryModificada(),
                    resultado.getParametros()
            );

            log.info("🔧 PARÁMETROS REALES PASADOS A JDBC:");
            for (String paramName : resultado.getParametros().getParameterNames()) {
                log.info("   {}: {}", paramName, resultado.getParametros().getValue(paramName));
            }
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