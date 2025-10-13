package org.transito_seguro.repository.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.model.query.QueryResult;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.exception.SQLExecutionException;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.utils.SQLExceptionParser;
import org.transito_seguro.utils.SqlUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

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
            QueryResult resultado = parametrosProcessor.procesarQuery(querySQL, filtros);

            // ✅ DEBUG 1: Query y parámetros
            log.debug("🔍 QUERY A EJECUTAR para {}:\n{}", provincia, resultado.getQueryModificada());
            log.debug("🔍 PARÁMETROS para {}: lastId={}, limite={}",
                    provincia,
                    filtros != null ? filtros.getLastId() : "null",
                    filtros != null ? filtros.getLimite() : "null");

            List<Map<String, Object>> resultados;
            try {
                resultados = jdbcTemplate.query(
                        resultado.getQueryModificada(),
                        resultado.getParametros(),
                        this::mapearFila
                );
            } catch (DataAccessException e) {
                SQLExecutionException executionException = SQLExceptionParser.parse(
                        e,
                        querySQL,
                        nombreQuery,
                        provincia
                );

                // Log con sugerencias específicas
                log.error(SQLExceptionParser.obtenerSugerencias(e, querySQL));
                log.error("{}", executionException.getMessageDetallado());

                throw executionException;
            }

            // ✅ DEBUG 2: Verificar QUÉ CAMPOS retornan los registros
            if (!resultados.isEmpty()) {
                Map<String, Object> primerRegistro = resultados.get(0);
                Map<String, Object> ultimoRegistro = resultados.get(resultados.size() - 1);

                log.debug("🔍 PRIMER REGISTRO {} - Campos: {}", provincia, primerRegistro.keySet());
                log.debug("🔍 PRIMER REGISTRO {} - Valores: {}", provincia, primerRegistro);

                log.debug("🔍 ÚLTIMO REGISTRO {} - Campos: {}", provincia, ultimoRegistro.keySet());
                log.debug("🔍 ÚLTIMO REGISTRO {} - Valores: {}", provincia, ultimoRegistro);

                // ✅ DEBUG 3: Buscar específicamente el campo 'id'
                if (primerRegistro.containsKey("id")) {
                    log.debug("✅ CAMPO 'id' ENCONTRADO en {}: valor={}", provincia, primerRegistro.get("id"));
                } else {
                    log.warn("⚠️ CAMPO 'id' NO ENCONTRADO en {}. Buscando alternativas...", provincia);

                    // Buscar cualquier campo que contenga "id"
                    for (String campo : primerRegistro.keySet()) {
                        if (campo.toLowerCase().contains("id")) {
                            log.debug("🔍 Campo similar a ID encontrado: {} = {}", campo, primerRegistro.get(campo));
                        }
                    }
                }
            } else {
                log.debug("🔍 {}: No hay resultados para analizar", provincia);
            }

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

        // ✅ DEBUG: Ver TODOS los campos del ResultSet
        if (rowNum == 0) { // Solo debug para el primer registro
            log.debug("🔍 CAMPOS EN RESULTSET para fila {}:", rowNum);
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String columnType = metaData.getColumnTypeName(i);
                log.debug("   - {} ({})", columnName, columnType);
            }
        }

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object columnValue = rs.getObject(i);

            // ✅ DEBUG: Campo 'id' específico
            if ("id".equalsIgnoreCase(columnName)) {
                log.debug("🎯 MAPEANDO CAMPO 'id': {}", columnValue);
            }

            // Verificar si estás filtrando algún campo
            // ❌ SI TIENES ESTO, QUÍTALO:
            // if (!"provincia".equalsIgnoreCase(columnName)) {
            //     row.put(columnName, columnValue);
            // }

            // ✅ DEBERÍA SER SIMPLEMENTE:
            row.put(columnName, columnValue);
        }

        // Solo agregar provincia si no existe
        if (!row.containsKey("provincia")) {
            row.put("provincia", this.provincia);
        }

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

    @Override
    public Integer ejecutarQueryConteo(String nombreQuery, ParametrosFiltrosDTO filtros) {
      try{

         QueryResult queryResult = parametrosProcessor.procesarQuery(nombreQuery,filtros);

         String queryFiltros = queryResult.getQueryModificada();

         return jdbcTemplate.queryForObject(queryFiltros, queryResult.getParametros(), Integer.class);

      }catch (Exception e){
          log.error("Error ejecutando query de conteo '{}' en provincia {}: {}",
                  nombreQuery, provincia, e.getMessage(), e);
          throw new RuntimeException("Error ejecutando query de conteo: " + nombreQuery, e);
      }
    }

    @Override
    public Integer ejecutarQueryConteoDesdeSQL(String sqlQuery) {
        try {
            // Procesar la query con filtros vacíos
            ParametrosFiltrosDTO filtrosVacios = new ParametrosFiltrosDTO();
            QueryResult queryResult = parametrosProcessor.procesarQuery(sqlQuery, filtrosVacios);

            String queryFiltros = queryResult.getQueryModificada();

            return jdbcTemplate.queryForObject(queryFiltros, queryResult.getParametros(), Integer.class);

        } catch (Exception e) {
            log.error("Error ejecutando query de conteo desde SQL en provincia {}: {}",
                    provincia, e.getMessage(), e);
            throw new RuntimeException("Error ejecutando query de conteo desde SQL", e);
        }
    }


    /**
 * Ejecuta una query usando streaming (cursor JDBC).
 * Procesa resultados uno por uno sin cargar todo en memoria.
 * 
 * IMPORTANTE: Solo usar para queries que pueden retornar muchos registros.
 * 
 * @param nombreQuery Código de la query
 * @param filtros Filtros aplicados
 * @param procesarRegistro Callback ejecutado por cada registro
 */
public void ejecutarQueryConStreaming(
        String nombreQuery,
        ParametrosFiltrosDTO filtros,
        Consumer<Map<String, Object>> procesarRegistro) {
    
    try {
        // Obtener query
        Optional<QueryStorage> queryOpt = queryStorageRepository.findByCodigo(nombreQuery);
        
        if (!queryOpt.isPresent()) {
            throw new RuntimeException("Query no encontrada: " + nombreQuery);
        }
        
        String sql = queryOpt.get().getSqlQuery();
        QueryResult queryResult = parametrosProcessor.procesarQuery(sql, filtros);
        String sqlModificada = queryResult.getQueryModificada();
        
        // ✅ CORRECCIÓN 1: getParametros() retorna MapSqlParameterSource, no Map
        org.springframework.jdbc.core.namedparam.MapSqlParameterSource parametros = 
            queryResult.getParametros();
        
        log.debug("Ejecutando query con streaming para provincia {}: {}", provincia, nombreQuery);
        
        // Convertir named parameters a posicionales
        org.springframework.jdbc.core.namedparam.ParsedSql parsedSql = 
            org.springframework.jdbc.core.namedparam.NamedParameterUtils.parseSqlStatement(sqlModificada);
        
        // ✅ CORRECCIÓN 2: Pasar 'parametros' directamente, no crear nuevo MapSqlParameterSource
        String sqlPosicional = org.springframework.jdbc.core.namedparam.NamedParameterUtils
            .substituteNamedParameters(parsedSql, parametros);
        
        // ✅ CORRECCIÓN 3: Pasar 'parametros' directamente
        Object[] args = org.springframework.jdbc.core.namedparam.NamedParameterUtils
            .buildValueArray(parsedSql, parametros, null);
        
        // Contador para logging
        final int[] contador = {0};
        
        // Usar JdbcTemplate nativo para tener control del PreparedStatement
        jdbcTemplate.getJdbcTemplate().query(
            connection -> {
                // Crear PreparedStatement con configuración de streaming
                java.sql.PreparedStatement ps = connection.prepareStatement(
                    sqlPosicional,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY
                );
                
                // CRÍTICO: Configurar fetch size para streaming
                ps.setFetchSize(1000);
                
                // Establecer parámetros
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
                
                return ps;
            },
            rs -> {
                // Este callback se ejecuta por cada fila
                Map<String, Object> registro = mapearResultSetStreaming(rs);
                procesarRegistro.accept(registro);
                
                // ✅ CORRECCIÓN 4: Agregar contador para logging
                contador[0]++;
                
                // Log cada 10,000 registros
                if (contador[0] % 10000 == 0) {
                    log.debug("Streaming provincia {}: {} registros procesados", 
                              provincia, contador[0]);
                }
            }
        );
        
        // ✅ CORRECCIÓN 5: Log con total de registros procesados
        log.debug("Query con streaming completada para provincia {}: {} registros totales", 
                  provincia, contador[0]);
        
    } catch (Exception e) {
        log.error("Error en streaming para provincia {}: {}", provincia, e.getMessage());
        throw new RuntimeException("Error en streaming: " + e.getMessage(), e);
    }
}

/**
 * Mapea una fila del ResultSet a un Map (versión para streaming).
 * 
 * @param rs ResultSet posicionado en la fila actual
 * @return Map con columnas y valores
 */
private Map<String, Object> mapearResultSetStreaming(ResultSet rs) {
    try {
        Map<String, Object> registro = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columna = metaData.getColumnName(i);
            Object valor = rs.getObject(i);
            
            // No incluir provincia si ya existe en el resultado
            if (!"provincia".equalsIgnoreCase(columna)) {
                registro.put(columna, valor);
            }
        }
        
        // Agregar provincia del repositorio
        registro.put("provincia", this.provincia);
        
        return registro;
        
    } catch (SQLException e) {
        log.error("Error mapeando ResultSet en streaming: {}", e.getMessage());
        throw new RuntimeException("Error mapeando fila", e);
    }
}

}