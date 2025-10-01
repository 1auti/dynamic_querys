package org.transito_seguro.component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class ParametrosProcessor {

    @Getter
    public static class QueryResult {
        private final String queryModificada;
        private final MapSqlParameterSource parametros;
        private final Map<String, Object> metadata;

        public QueryResult(String queryModificada, MapSqlParameterSource parametros, Map<String, Object> metadata) {
            this.queryModificada = queryModificada;
            this.parametros = parametros;
            this.metadata = metadata;
        }
    }

    /**
     * Método principal para procesar cualquier query con filtros dinámicos
     * CORREGIDO: Tipos explícitos seguros para PostgreSQL
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        // Mapear TODOS los parámetros
        mapearParametroFechasSeguro(filtros, parametros);
        mapearParametrosUbicacion(filtros, parametros);
        mapearParametrosEquipos(filtros, parametros);
        mapearParametrosInfracciones(filtros, parametros);
        mapearParametrosDominios(filtros, parametros);
        mapearParametrosAdicionalesSeguro(filtros, parametros);

        // ✅ CRÍTICO: Mapear Keyset Y paginación
        mapearParametrosKeyset(parametros, filtros);
        mapearPaginacionKeyset(parametros, filtros);

        log.debug("Query procesada con Keyset. Parámetros: {}", parametros.getParameterNames().length);

        return new QueryResult(queryOriginal, parametros, metadata);
    }

    // =================== MAPEO DE FECHAS CORREGIDO ===================

    /**
     * NUEVO: Mapeo de fechas más seguro para PostgreSQL
     */
    private void mapearParametroFechasSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Fechas principales con tipos específicos
        params.addValue("fechaInicio", filtros.getFechaInicio(), Types.DATE);
        params.addValue("fechaFin", filtros.getFechaFin(), Types.DATE);
        params.addValue("fechaEspecifica", filtros.getFechaEspecifica(), Types.DATE);
    }

    // =================== MAPEO DE UBICACIÓN CORREGIDO ===================

    private void mapearParametrosUbicacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Ubicación geográfica con tipos específicos para PostgreSQL
        params.addValue("provincias", convertirListaAArrayPostgreSQL(filtros.getProvincias()), Types.OTHER);
        params.addValue("municipios", convertirListaAArrayPostgreSQL(filtros.getMunicipios()), Types.OTHER);
        params.addValue("lugares", convertirListaAArrayPostgreSQL(filtros.getLugares()), Types.OTHER);
        params.addValue("partido", convertirListaAArrayPostgreSQL(filtros.getPartido()), Types.OTHER);

        // Arrays de enteros para PostgreSQL
        params.addValue("concesiones", convertirListaEnterosAArrayPostgreSQL(filtros.getConcesiones()), Types.OTHER);
    }

    private void mapearPaginacion(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("limite", 1000, Types.INTEGER);
            params.addValue("offset", 0, Types.INTEGER);
            return;
        }

        // Si tiene Keyset, no usar OFFSET
        if (filtros.getLastId() != null) {
            params.addValue("limite", filtros.getLimite(), Types.INTEGER);
            params.addValue("offset", null, Types.INTEGER); // Ignorar OFFSET
            log.debug("Paginación Keyset: limite={}", filtros.getLimite());
        } else {
            // Paginación OFFSET normal
            params.addValue("limite", filtros.getLimite(), Types.INTEGER);
            params.addValue("offset", filtros.getOffset(), Types.INTEGER);
            log.debug("Paginación OFFSET: limite={}, offset={}",
                    filtros.getLimite(), filtros.getOffset());
        }
    }


    // =================== MAPEO DE EQUIPOS CORREGIDO ===================

    private void mapearParametrosEquipos(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Tipos de dispositivos
        params.addValue("tiposDispositivos", convertirListaEnterosAArrayPostgreSQL(filtros.getTiposDispositivos()), Types.OTHER);

        // Patrones para búsqueda con LIKE
        params.addValue("patronesEquipos", convertirListaAArrayPostgreSQL(filtros.getPatronesEquipos()), Types.OTHER);
        params.addValue("tipoEquipo", convertirListaAArrayPostgreSQL(filtros.getPatronesEquipos()), Types.OTHER); // Alias

        // Series exactas de equipos
        params.addValue("seriesEquiposExactas", convertirListaAArrayPostgreSQL(filtros.getSeriesEquiposExactas()), Types.OTHER);

        // Filtros booleanos para tipos específicos de equipos
        params.addValue("filtrarPorTipoEquipo", filtros.getFiltrarPorTipoEquipo(), Types.BOOLEAN);
        params.addValue("incluirSE", filtros.getIncluirSE(), Types.BOOLEAN);
        params.addValue("incluirVLR", filtros.getIncluirVLR(), Types.BOOLEAN);
    }

    // =================== MAPEO DE INFRACCIONES CORREGIDO ===================

    private void mapearParametrosInfracciones(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Tipos y estados
        params.addValue("tiposInfracciones", convertirListaEnterosAArrayPostgreSQL(filtros.getTiposInfracciones()), Types.OTHER);
        params.addValue("estadosInfracciones", convertirListaEnterosAArrayPostgreSQL(filtros.getEstadosInfracciones()), Types.OTHER);

        // Exportación SACIT
        params.addValue("exportadoSacit", filtros.getExportadoSacit(), Types.BOOLEAN);
    }

    // =================== MAPEO DE DOMINIOS Y VEHÍCULOS CORREGIDO ===================

    private void mapearParametrosDominios(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposVehiculos", convertirListaAArrayPostgreSQL(filtros.getTipoVehiculo()), Types.OTHER);
        params.addValue("tieneEmail", filtros.getTieneEmail(), Types.BOOLEAN);

        // Parámetro específico para la query de personas jurídicas
        params.addValue("tipoDocumento", null, Types.VARCHAR);
    }

    // =================== MAPEO DE PARÁMETROS ADICIONALES CORREGIDO ===================

    /**
     * NUEVO: Parámetros adicionales más seguros para PostgreSQL
     */
    private void mapearParametrosAdicionalesSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Parámetros que pueden ser nulos pero son necesarios para algunas queries
        params.addValue("provincia", null, Types.VARCHAR);
        params.addValue("fechaReporte", null, Types.DATE);
    }

    // =================== MAPEO DE PAGINACIÓN CORREGIDO ===================

    /**
     * NUEVO: Paginación más segura para PostgreSQL
     */
    private void mapearPaginacionSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Asegurar que limite y offset nunca sean null
        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        params.addValue("limite", limite > 0 ? limite : 1000, Types.INTEGER);
        params.addValue("offset", offset >= 0 ? offset : 0, Types.INTEGER);
    }

    private void mapearParametrosKeyset(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("lastId", null, Types.INTEGER);
            params.addValue("lastSerieEquipo", null, Types.VARCHAR);
            params.addValue("lastLugar", null, Types.VARCHAR);
            return;
        }

        // Mapear con tipos explícitos
        params.addValue("lastId", filtros.getLastId(), Types.INTEGER);
        params.addValue("lastSerieEquipo", filtros.getLastSerieEquipo(), Types.VARCHAR);
        params.addValue("lastLugar", filtros.getLastLugar(), Types.VARCHAR);

        if (filtros.getLastId() != null) {
            log.debug("🔑 Keyset activo: lastId={}, lastSerie={}, lastLugar={}",
                    filtros.getLastId(), filtros.getLastSerieEquipo(), filtros.getLastLugar());
        }
    }

    /**
     * ✅ NUEVO: Paginación sin OFFSET cuando hay Keyset
     */
    private void mapearPaginacionKeyset(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("limite", 1000, Types.INTEGER);
            return;
        }

        // Solo LIMIT, nunca OFFSET con Keyset
        int limite = filtros.getLimiteEfectivo();
        params.addValue("limite", limite > 0 ? limite : 1000, Types.INTEGER);

        log.debug("📊 Paginación: limite={} (sin OFFSET)", limite);
    }
    // =================== MÉTODOS UTILITARIOS CORREGIDOS ===================

    /**
     * Convierte una lista de strings a un arreglo compatible con PostgreSQL
     * CORREGIDO: Retorna null explícitamente si está vacío
     */
    private String[] convertirListaAArrayPostgreSQL(List<String> lista) {
        if (lista == null || lista.isEmpty()) {
            return null; // PostgreSQL entiende NULL mejor que array vacío
        }
        return lista.toArray(new String[0]);
    }

    /**
     * Convierte una lista de enteros a un arreglo compatible con PostgreSQL
     * CORREGIDO: Retorna null explícitamente si está vacío
     */
    private Integer[] convertirListaEnterosAArrayPostgreSQL(List<Integer> lista) {
        if (lista == null || lista.isEmpty()) {
            return null; // PostgreSQL entiende NULL mejor que array vacío
        }
        return lista.toArray(new Integer[0]);
    }

    // =================== MÉTODOS LEGACY (MANTENER PARA COMPATIBILIDAD) ===================

    /**
     * @deprecated Mantener para compatibilidad - usar métodos específicos de PostgreSQL
     */
    @Deprecated
    private String[] convertirListaAArray(List<String> lista) {
        return convertirListaAArrayPostgreSQL(lista);
    }

    /**
     * @deprecated Mantener para compatibilidad - usar métodos específicos de PostgreSQL
     */
    @Deprecated
    private Integer[] convertirListaEnterosAArray(List<Integer> lista) {
        return convertirListaEnterosAArrayPostgreSQL(lista);
    }

    /**
     * @deprecated Usar procesarQuery() con tipos seguros
     */
    @Deprecated
    private void mapearParametroFechas(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        mapearParametroFechasSeguro(filtros, params);
    }

    /**
     * @deprecated Usar mapearPaginacionSeguro()
     */
    @Deprecated
    private void mapearPaginacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        mapearPaginacionSeguro(filtros, params);
    }

    /**
     * @deprecated Usar mapearParametrosAdicionalesSeguro()
     */
    @Deprecated
    private void mapearParametrosAdicionales(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        mapearParametrosAdicionalesSeguro(filtros, params);
    }

    // =================== MÉTODOS DE DEBUGGING Y VALIDACIÓN ===================

    /**
     * Método utilitario para debugging de parámetros
     */
    public void logParametros(MapSqlParameterSource params) {
        if (log.isDebugEnabled()) {
            for (String paramName : params.getParameterNames()) {
                Object value = params.getValue(paramName);
                log.debug("Parámetro: {} = {} ({})",
                        paramName,
                        value,
                        value != null ? value.getClass().getSimpleName() : "null"
                );
            }
        }
    }

    /**
     * Valida que todos los parámetros requeridos estén presentes
     */
    public boolean validarParametrosRequeridos(MapSqlParameterSource params, String... nombresRequeridos) {
        for (String nombre : nombresRequeridos) {
            if (!params.hasValue(nombre)) {
                log.warn("Parámetro requerido faltante: {}", nombre);
                return false;
            }
        }
        return true;
    }

    /**
     * Obtiene estadísticas de parámetros mapeados
     */
    public Map<String, Object> obtenerEstadisticasParametros(MapSqlParameterSource params) {
        Map<String, Object> stats = new HashMap<>();

        int totalParametros = params.getParameterNames().length;
        int parametrosConValor = 0;
        int parametrosNulos = 0;

        for (String paramName : params.getParameterNames()) {
            Object value = params.getValue(paramName);
            if (value != null) {
                parametrosConValor++;
            } else {
                parametrosNulos++;
            }
        }

        stats.put("total_parametros", totalParametros);
        stats.put("parametros_con_valor", parametrosConValor);
        stats.put("parametros_nulos", parametrosNulos);
        stats.put("porcentaje_utilizacion",
                totalParametros > 0 ? (double) parametrosConValor / totalParametros * 100 : 0);

        return stats;
    }

    /**
     * Valida tipos de parámetros para PostgreSQL
     */
    public boolean validarTiposPostgreSQL(MapSqlParameterSource params) {
        boolean todosValidos = true;

        for (String paramName : params.getParameterNames()) {
            Object value = params.getValue(paramName);
            int sqlType = params.getSqlType(paramName);

            if (value != null && sqlType == Types.NULL) {
                log.warn("Parámetro '{}' sin tipo SQL explícito", paramName);
                todosValidos = false;
            }
        }

        return todosValidos;
    }
}