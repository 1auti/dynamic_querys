package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.query.QueryResult;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class ParametrosProcessor {

    /**
     * Método principal para procesar cualquier query con filtros dinámicos
     * - Solo lastId para keyset
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        // 1. Mapear TODOS los parámetros básicos
        mapearParametroFechasSeguro(filtros, parametros);
        mapearParametrosUbicacion(filtros, parametros);
        mapearParametrosEquipos(filtros, parametros);
        mapearParametrosInfracciones(filtros, parametros);
        mapearParametrosDominios(filtros, parametros);
        mapearParametrosAdicionalesSeguro(filtros, parametros);

        // 2.  KEYSET SIMPLIFICADO + PAGINACIÓN
        mapearKeysetSimplificado(parametros, filtros);
        mapearPaginacionKeyset(parametros, filtros);

        log.debug("Query procesada. Parámetros: {} | Keyset: {}", 
                  parametros.getParameterNames().length,
                  filtros.getLastId() != null ? "ACTIVO(id=" + filtros.getLastId() + ")" : "INACTIVO");

        return new QueryResult(queryOriginal, parametros, metadata);
    }

    // =================== KEYSET SIMPLIFICADO ===================

    /**
     *  KEYSET SIMPLIFICADO - Solo lastId para todas las queries
     */
    private void mapearKeysetSimplificado(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("lastId", null, Types.INTEGER);
            return;
        }

        //  SOLO lastId - suficiente para paginación eficiente
        params.addValue("lastId", filtros.getLastId(), Types.INTEGER);
        
        if (filtros.getLastId() != null) {
            log.debug("🔑 Keyset SIMPLIFICADO activo: lastId={}", filtros.getLastId());
        }
    }

    /**
     *  PAGINACIÓN SOLO CON LIMIT (sin OFFSET cuando hay keyset)
     */
    private void mapearPaginacionKeyset(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("limite", 1000, Types.INTEGER);
            return;
        }

        //  Solo LIMIT, nunca OFFSET con Keyset
        int limite = filtros.getLimiteEfectivo();
        params.addValue("limite", limite > 0 ? limite : 1000, Types.INTEGER);

        log.debug("📊 Paginación Keyset: limite={}", limite);
    }

    // =================== MAPEO DE FECHAS ===================

    private void mapearParametroFechasSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("fechaInicio", filtros.getFechaInicio(), Types.DATE);
        params.addValue("fechaFin", filtros.getFechaFin(), Types.DATE);
        params.addValue("fechaEspecifica", filtros.getFechaEspecifica(), Types.DATE);
    }

    // =================== MAPEO DE UBICACIÓN ===================

    private void mapearParametrosUbicacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("provincias", convertirListaAArrayPostgreSQL(filtros.getProvincias()), Types.OTHER);
        params.addValue("municipios", convertirListaAArrayPostgreSQL(filtros.getMunicipios()), Types.OTHER);
        params.addValue("lugares", convertirListaAArrayPostgreSQL(filtros.getLugares()), Types.OTHER);
        params.addValue("partido", convertirListaAArrayPostgreSQL(filtros.getPartido()), Types.OTHER);
        params.addValue("concesiones", convertirListaEnterosAArrayPostgreSQL(filtros.getConcesiones()), Types.OTHER);
    }

    // =================== MAPEO DE EQUIPOS ===================

    private void mapearParametrosEquipos(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposDispositivos", convertirListaEnterosAArrayPostgreSQL(filtros.getTiposDispositivos()), Types.OTHER);
        params.addValue("patronesEquipos", convertirListaAArrayPostgreSQL(filtros.getPatronesEquipos()), Types.OTHER);
        params.addValue("tipoEquipo", convertirListaAArrayPostgreSQL(filtros.getPatronesEquipos()), Types.OTHER);
        params.addValue("seriesEquiposExactas", convertirListaAArrayPostgreSQL(filtros.getSeriesEquiposExactas()), Types.OTHER);
        params.addValue("filtrarPorTipoEquipo", filtros.getFiltrarPorTipoEquipo(), Types.BOOLEAN);
        params.addValue("incluirSE", filtros.getIncluirSE(), Types.BOOLEAN);
        params.addValue("incluirVLR", filtros.getIncluirVLR(), Types.BOOLEAN);
    }

    // =================== MAPEO DE INFRACCIONES ===================

    private void mapearParametrosInfracciones(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposInfracciones", convertirListaEnterosAArrayPostgreSQL(filtros.getTiposInfracciones()), Types.OTHER);
        params.addValue("estadosInfracciones", convertirListaEnterosAArrayPostgreSQL(filtros.getEstadosInfracciones()), Types.OTHER);
        params.addValue("exportadoSacit", filtros.getExportadoSacit(), Types.BOOLEAN);
    }

    // =================== MAPEO DE DOMINIOS ===================

    private void mapearParametrosDominios(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposVehiculos", convertirListaAArrayPostgreSQL(filtros.getTipoVehiculo()), Types.OTHER);
        params.addValue("tieneEmail", filtros.getTieneEmail(), Types.BOOLEAN);
        params.addValue("tipoDocumento", null, Types.VARCHAR);
    }

    // =================== PARÁMETROS ADICIONALES ===================

    private void mapearParametrosAdicionalesSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("provincia", null, Types.VARCHAR);
        params.addValue("fechaReporte", null, Types.DATE);
    }

    // =================== MÉTODOS UTILITARIOS ===================

    private String[] convertirListaAArrayPostgreSQL(List<String> lista) {
        if (lista == null || lista.isEmpty()) {
            return null;
        }
        return lista.toArray(new String[0]);
    }

    private Integer[] convertirListaEnterosAArrayPostgreSQL(List<Integer> lista) {
        if (lista == null || lista.isEmpty()) {
            return null;
        }
        return lista.toArray(new Integer[0]);
    }

    // =================== MÉTODOS DELEGACY (ELIMINAR EVENTUALMENTE) ===================

    /**
     * @deprecated Usar mapearKeysetSimplificado
     */
    @Deprecated
    private void mapearParametrosKeyset(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        mapearKeysetSimplificado(params, filtros);
    }

    /**
     * @deprecated Usar mapearPaginacionKeyset
     */
    @Deprecated
    private void mapearPaginacionSeguro(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        mapearPaginacionKeyset(params, filtros);
    }

    /**
     * @deprecated Mantener para compatibilidad
     */
    @Deprecated
    private void mapearPaginacion(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        mapearPaginacionKeyset(params, filtros);
    }

    // =================== MÉTODOS DE DEBUGGING ===================

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

    public boolean validarParametrosRequeridos(MapSqlParameterSource params, String... nombresRequeridos) {
        for (String nombre : nombresRequeridos) {
            if (!params.hasValue(nombre)) {
                log.warn("Parámetro requerido faltante: {}", nombre);
                return false;
            }
        }
        return true;
    }
}