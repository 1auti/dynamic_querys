package org.transito_seguro.repository.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.utils.SqlUtils;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implementación del repositorio de infracciones para una provincia específica.
 * Maneja la ejecución de consultas SQL dinámicas con filtros parametrizados.
 * Soporta carga de queries desde base de datos o archivos.
 */
@Slf4j
public class InfraccionesRepositoryImpl implements InfraccionesRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Getter
    private final String provincia;

    private final ParametrosProcessor parametrosProcessor;

    @Setter
    private QueryStorageRepository queryStorageRepository;

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
            String querySQL = cargarQuery(nombreQuery);
            ParametrosProcessor.QueryResult resultado = parametrosProcessor.procesarQuery(querySQL, filtros);

            List<Map<String, Object>> resultados = jdbcTemplate.query(
                    resultado.getQueryModificada(),
                    resultado.getParametros(),
                    this::mapearFila
            );

//            logParametros(resultado);
            log.debug("Query '{}' completada. Resultados: {} registros", nombreQuery, resultados.size());

            return resultados;

        } catch (Exception e) {
            log.error("Error ejecutando query '{}' en provincia {}: {}",
                    nombreQuery, provincia, e.getMessage(), e);
            throw new RuntimeException("Error ejecutando query: " + nombreQuery, e);
        }
    }

    private Map<String, Object> mapearFila(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);

            if (!"provincia".equalsIgnoreCase(columnName)) {
                row.put(columnName, rs.getObject(i));
            }
        }

        row.put("provincia", this.provincia);
        return row;
    }

    private String cargarQuery(String nombreQuery) {
        // Intentar cargar desde base de datos
        if (queryStorageRepository != null) {
            try {
                Optional<QueryStorage> queryStorage = queryStorageRepository.findByCodigo(nombreQuery);

                if (queryStorage.isPresent() && queryStorage.get().estaLista()) {
                    log.debug("Query '{}' cargada desde BD", nombreQuery);
                    return queryStorage.get().getSqlQuery();
                }

            } catch (Exception e) {
                log.warn("Error accediendo BD para query '{}': {}", nombreQuery, e.getMessage());
            }
        }

        // Fallback: cargar desde archivo
        try {
            log.debug("Query '{}' cargada desde archivo", nombreQuery);
            return SqlUtils.cargarQuery(nombreQuery);

        } catch (Exception e) {
            throw new RuntimeException("Query no encontrada: " + nombreQuery +
                    ". No existe en BD ni en archivos.", e);
        }
    }

//    private void logParametros(ParametrosProcessor.QueryResult resultado) {
//        if (log.isInfoEnabled()) {
//            log.info("Parámetros pasados a JDBC:");
//            for (String paramName : resultado.getParametros().getParameterNames()) {
//                log.info("  {}: {}", paramName, resultado.getParametros().getValue(paramName));
//            }
//        }
//    }

    public boolean validarConectividad() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", new HashMap<>(), Integer.class);
            log.debug("Conectividad validada para provincia: {}", provincia);
            return true;
        } catch (Exception e) {
            log.warn("Error validando conectividad para provincia {}: {}", provincia, e.getMessage());
            return false;
        }
    }

    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
        return jdbcTemplate;
    }

    @Override
    public String toString() {
        return String.format("InfraccionesRepositoryImpl{provincia='%s'}", provincia);
    }
}