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
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        // Mapear TODOS los parámetros posibles con tipos específicos
        mapearParametroFechas(filtros, parametros);
        mapearParametrosUbicacion(filtros, parametros);
        mapearParametrosEquipos(filtros, parametros);
        mapearParametrosInfracciones(filtros, parametros);
        mapearParametrosDominios(filtros, parametros);
        mapearParametrosAdicionales(filtros, parametros);
        mapearPaginacion(filtros, parametros);

        log.debug("Query procesada. Parámetros mapeados: {}", parametros.getParameterNames().length);

        return new QueryResult(queryOriginal, parametros, metadata);
    }

    // =================== MAPEO DE FECHAS ===================

    private void mapearParametroFechas(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Fechas principales con tipos específicos
        params.addValue("fechaInicio", filtros.getFechaInicio(), Types.DATE);
        params.addValue("fechaFin", filtros.getFechaFin(), Types.DATE);
        params.addValue("fechaEspecifica", filtros.getFechaEspecifica(), Types.DATE);
    }

    // =================== MAPEO DE UBICACIÓN ===================

    private void mapearParametrosUbicacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Ubicación geográfica con tipos específicos
        params.addValue("provincias", convertirListaAArray(filtros.getProvincias()), Types.ARRAY);
        params.addValue("municipios", convertirListaAArray(filtros.getMunicipios()), Types.ARRAY);
        params.addValue("lugares", convertirListaAArray(filtros.getLugares()), Types.ARRAY);
        params.addValue("partido", convertirListaAArray(filtros.getPartido()), Types.ARRAY);

        // Arrays de enteros
        params.addValue("concesiones", convertirListaEnterosAArray(filtros.getConcesiones()), Types.ARRAY);
    }

    // =================== MAPEO DE EQUIPOS ===================

    private void mapearParametrosEquipos(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Tipos de dispositivos
        params.addValue("tiposDispositivos", convertirListaEnterosAArray(filtros.getTiposDispositivos()), Types.ARRAY);

        // Patrones para búsqueda con LIKE
        params.addValue("patronesEquipos", convertirListaAArray(filtros.getPatronesEquipos()), Types.ARRAY);
        params.addValue("tipoEquipo", convertirListaAArray(filtros.getPatronesEquipos()), Types.ARRAY); // Alias

        // Series exactas de equipos
        params.addValue("seriesEquiposExactas", convertirListaAArray(filtros.getSeriesEquiposExactas()), Types.ARRAY);

        // Filtros booleanos para tipos específicos de equipos
        params.addValue("filtrarPorTipoEquipo", filtros.getFiltrarPorTipoEquipo(), Types.BOOLEAN);
        params.addValue("incluirSE", filtros.getIncluirSE(), Types.BOOLEAN);
        params.addValue("incluirVLR", filtros.getIncluirVLR(), Types.BOOLEAN);
    }

    // =================== MAPEO DE INFRACCIONES ===================

    private void mapearParametrosInfracciones(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Tipos y estados
        params.addValue("tiposInfracciones", convertirListaEnterosAArray(filtros.getTiposInfracciones()), Types.ARRAY);
        params.addValue("estadosInfracciones", convertirListaEnterosAArray(filtros.getEstadosInfracciones()), Types.ARRAY);

        // Exportación SACIT
        params.addValue("exportadoSacit", filtros.getExportadoSacit(), Types.BOOLEAN);
    }

    // =================== MAPEO DE DOMINIOS Y VEHÍCULOS ===================

    private void mapearParametrosDominios(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposVehiculos", convertirListaAArray(filtros.getTipoVehiculo()), Types.ARRAY);
        params.addValue("tieneEmail", filtros.getTieneEmail(), Types.BOOLEAN);

        // Parámetro específico para la query de personas jurídicas
        params.addValue("tipoDocumento", null, Types.VARCHAR);
    }

    // =================== MAPEO DE PARÁMETROS ADICIONALES ===================

    private void mapearParametrosAdicionales(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Parámetros que pueden ser nulos pero son necesarios para algunas queries
        params.addValue("provincia", null, Types.VARCHAR); // Se setea dinámicamente por el repositorio
        params.addValue("fechaReporte", null, Types.DATE);  // Para reportes específicos
    }

    // =================== MAPEO DE PAGINACIÓN ===================

    private void mapearPaginacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("limite", filtros.getLimiteMaximo(), Types.INTEGER);
        params.addValue("offset", filtros.calcularOffset(), Types.INTEGER);
    }

    // =================== MÉTODOS UTILITARIOS ===================

    /**
     * Convierte una lista de strings a un arreglo compatible con PostgreSQL
     */
    private String[] convertirListaAArray(List<String> lista) {
        if (lista == null || lista.isEmpty()) {
            return null;
        }
        return lista.toArray(new String[0]);
    }

    /**
     * Convierte una lista de enteros a un arreglo compatible con PostgreSQL
     */
    private Integer[] convertirListaEnterosAArray(List<Integer> lista) {
        if (lista == null || lista.isEmpty()) {
            return null;
        }
        return lista.toArray(new Integer[0]);
    }

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
}