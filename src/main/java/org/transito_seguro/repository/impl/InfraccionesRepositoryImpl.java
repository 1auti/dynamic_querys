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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * 🔥 VERSIÓN MEJORADA: Con streaming real y logs de progreso
     *
     * CAMBIOS PRINCIPALES:
     * 1. Logs ANTES de iniciar la query
     * 2. Logs cada 1000 registros DURANTE la lectura
     * 3. Logs DESPUÉS con estadísticas finales
     * 4. Manejo de streaming eficiente
     */
    @Override
    public List<Map<String, Object>> ejecutarQueryConFiltros(String nombreQuery, ParametrosFiltrosDTO filtros) {

        // ✅ 1. LOG INICIAL - Para saber que empezó
        log.info("🔹 {} - Preparando query '{}'", provincia, nombreQuery);

        try {
            String querySQL = cargarQuery(nombreQuery);
            QueryResult resultado = parametrosProcessor.procesarQuery(querySQL, filtros);

            // ✅ 2. LOG DE QUERY Y PARÁMETROS
            log.debug("🔍 QUERY para {}:\n{}", provincia, resultado.getQueryModificada());
            log.info("🔍 {} - Parámetros: lastId={}, limite={}",
                    provincia,
                    filtros != null ? filtros.getLastId() : "null",
                    filtros != null ? filtros.getLimite() : "null");

            // ✅ 3. CONTADORES PARA PROGRESO EN TIEMPO REAL
            AtomicInteger registrosLeidos = new AtomicInteger(0);
            long inicioQuery = System.currentTimeMillis();

            // ✅ 4. LOG ANTES DE EJECUTAR
            log.info("🚀 {} - INICIANDO lectura de datos...", provincia);

            List<Map<String, Object>> resultados;
            try {
                // ✅ 5. EJECUTAR CON CALLBACK DE PROGRESO
                resultados = jdbcTemplate.query(
                        resultado.getQueryModificada(),
                        resultado.getParametros(),
                        rs -> {
                            List<Map<String, Object>> lista = new ArrayList<>();
                            ResultSetMetaData metaData = rs.getMetaData();
                            int columnCount = metaData.getColumnCount();

                            // ✅ 6. LEER FILA POR FILA (STREAMING REAL)
                            while (rs.next()) {
                                Map<String, Object> row = mapearFila(rs, metaData, columnCount);
                                lista.add(row);

                                int leidos = registrosLeidos.incrementAndGet();

                                // ✅ 7. LOG DE PROGRESO cada 1000 registros
                                if (leidos % 1000 == 0) {
                                    long transcurrido = System.currentTimeMillis() - inicioQuery;
                                    double velocidad = leidos * 1000.0 / Math.max(transcurrido, 1);

                                    log.info("📊 {} - Leyendo: {} registros en {}ms ({:.0f} reg/s)",
                                            provincia, leidos, transcurrido, velocidad);
                                }
                            }

                            return lista;
                        }
                );

            } catch (DataAccessException e) {
                SQLExecutionException executionException = SQLExceptionParser.parse(
                        e,
                        querySQL,
                        nombreQuery,
                        provincia
                );

                log.error(SQLExceptionParser.obtenerSugerencias(e, querySQL));
                log.error("{}", executionException.getMessageDetallado());

                throw executionException;
            }

            // ✅ 8. ESTADÍSTICAS FINALES
            long duracionTotal = System.currentTimeMillis() - inicioQuery;
            double velocidadPromedio = resultados.size() * 1000.0 / Math.max(duracionTotal, 1);

            log.info("✅ {} - Query completada: {} registros en {}ms ({:.0f} reg/s)",
                    provincia, resultados.size(), duracionTotal, velocidadPromedio);

            // ✅ 9. ANÁLISIS DE ESTRUCTURA (solo para debugging)
            if (log.isDebugEnabled() && !resultados.isEmpty()) {
                analizarEstructuraResultados(resultados);
            }

            return resultados;

        } catch (Exception e) {
            log.error("❌ {} - Error ejecutando query '{}': {}",
                    provincia, nombreQuery, e.getMessage(), e);
            throw new RuntimeException("Error ejecutando query: " + nombreQuery, e);
        }
    }



    /**
     * 📊 Analiza estructura de resultados (solo en modo DEBUG)
     */
    private void analizarEstructuraResultados(List<Map<String, Object>> resultados) {
        Map<String, Object> primerRegistro = resultados.get(0);
        Map<String, Object> ultimoRegistro = resultados.get(resultados.size() - 1);

        log.debug("🔍 PRIMER REGISTRO {} - Campos: {}", provincia, primerRegistro.keySet());
        log.debug("🔍 PRIMER REGISTRO {} - Valores: {}", provincia, primerRegistro);

        log.debug("🔍 ÚLTIMO REGISTRO {} - Campos: {}", provincia, ultimoRegistro.keySet());
        log.debug("🔍 ÚLTIMO REGISTRO {} - Valores: {}", provincia, ultimoRegistro);

        // Buscar campo 'id'
        if (primerRegistro.containsKey("id")) {
            log.debug("✅ Campo 'id' encontrado en {}: valor={}",
                    provincia, primerRegistro.get("id"));
        } else {
            log.warn("⚠️ Campo 'id' NO encontrado en {}. Campos similares:", provincia);

            for (String campo : primerRegistro.keySet()) {
                if (campo.toLowerCase().contains("id")) {
                    log.debug("   🔍 {} = {}", campo, primerRegistro.get(campo));
                }
            }
        }
    }


    /**
     * 🔧 Mapea una fila del ResultSet a Map
     * (Versión optimizada del método original mapearFila)
     */
    private Map<String, Object> mapearFila(ResultSet rs, ResultSetMetaData metaData, int columnCount)
            throws SQLException {

        Map<String, Object> row = new LinkedHashMap<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            row.put(columnName, value);
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
     * 🌊 Ejecuta query con streaming real usando NamedParameterJdbcTemplate.
     *
     * SIMPLIFICADO: Usa directamente NamedParameterJdbcTemplate que maneja
     * automáticamente los parámetros nombrados.
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
            // 1. Cargar y preparar query
            String sql = cargarQuery(nombreQuery);
            QueryResult queryResult = parametrosProcessor.procesarQuery(sql, filtros);
            String sqlModificada = queryResult.getQueryModificada();
            MapSqlParameterSource parametros = queryResult.getParametros();

            log.info("🌊 STREAMING INICIADO para {} - Query: {}", provincia, nombreQuery);

            // 2. Contadores
            AtomicInteger contador = new AtomicInteger(0);
            long inicioStreaming = System.currentTimeMillis();
            long ultimoLog = inicioStreaming;

            // 3. ✅ SOLUCIÓN SIMPLE: Usar NamedParameterJdbcTemplate directamente
            jdbcTemplate.query(
                    sqlModificada,
                    parametros,
                    rs -> {
                        try {
                            // Mapear fila
                            Map<String, Object> registro = mapearResultSetStreaming(rs);

                            // Procesar inmediatamente
                            procesarRegistro.accept(registro);

                            // Incrementar contador
                            int count = contador.incrementAndGet();

                            // Log cada 10,000 registros
                            if (count % 10000 == 0) {
                                long ahora = System.currentTimeMillis();
                                long transcurrido = ahora - inicioStreaming;
                                double velocidad = count * 1000.0 / Math.max(transcurrido, 1);

                                log.info("🌊 Streaming {}: {} registros ({:.0f} reg/s)",
                                        provincia, count, velocidad);
                            }

                        } catch (Exception e) {
                            log.error("❌ Error procesando registro en streaming: {}", e.getMessage());
                            // Continuar con siguiente registro
                        }
                    }
            );

            // 4. Estadísticas finales
            long duracionTotal = System.currentTimeMillis() - inicioStreaming;
            double velocidadPromedio = contador.get() * 1000.0 / Math.max(duracionTotal, 1);

            log.info("✅ STREAMING COMPLETADO para {}: {} registros en {}ms ({:.0f} reg/s)",
                    provincia, contador.get(), duracionTotal, velocidadPromedio);

        } catch (DataAccessException e) {
            SQLExecutionException executionException = SQLExceptionParser.parse(
                    e,
                    nombreQuery,
                    nombreQuery,
                    provincia
            );

            log.error("💥 Error en streaming para {}: {}",
                    provincia, executionException.getMessageDetallado());
            throw new RuntimeException("Error en streaming: " + e.getMessage(), e);

        } catch (Exception e) {
            log.error("💥 Error inesperado en streaming para {}: {}", provincia, e.getMessage(), e);
            throw new RuntimeException("Error en streaming: " + e.getMessage(), e);
        }
    }

    /**
     * Mapea una fila del ResultSet a Map (versión para streaming).
     *
     * @param rs ResultSet posicionado en la fila actual
     * @return Map con columnas y valores
     */
    private Map<String, Object> mapearResultSetStreaming(ResultSet rs) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Pre-dimensionar HashMap para performance
            Map<String, Object> registro = new HashMap<>((int)((columnCount + 1) / 0.75f) + 1);

            boolean tieneColumnaProvinciaEnResultSet = false;

            for (int i = 1; i <= columnCount; i++) {
                String columna = metaData.getColumnLabel(i);
                Object valor = rs.getObject(i);

                // Detectar si hay columna "provincia" en el ResultSet
                if ("provincia".equalsIgnoreCase(columna)) {
                    tieneColumnaProvinciaEnResultSet = true;
                    registro.put("provincia", valor != null ? valor : this.provincia);
                    continue;
                }

                // Agregar columna (incluso si es null)
                registro.put(columna, valor);
            }

            // Agregar provincia si NO estaba en el ResultSet
            if (!tieneColumnaProvinciaEnResultSet) {
                registro.put("provincia", this.provincia);
            }

            return registro;

        } catch (SQLException e) {
            log.error("❌ Error mapeando ResultSet: {}", e.getMessage());
            throw new RuntimeException("Error mapeando fila", e);
        }
    }

}