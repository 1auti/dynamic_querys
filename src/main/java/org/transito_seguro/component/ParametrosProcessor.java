package org.transito_seguro.component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

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

        // Mapear TODOS los parámetros posibles
        mapearParametroFechas(filtros, parametros);
        mapearParametrosUbicacion(filtros, parametros);
        mapearParametrosEquipos(filtros, parametros);
        mapearParametrosInfracciones(filtros, parametros);
        mapearParametrosDominios(filtros, parametros);
        mapearPaginacion(filtros, parametros);

        log.debug("Query procesada. Parámetros mapeados: {}", parametros.getParameterNames().length);

        return new QueryResult(queryOriginal, parametros, metadata);
    }

    // =================== MAPEO DE FECHAS ===================

    private void mapearParametroFechas(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Fechas principales
        params.addValue("fechaInicio", filtros.getFechaInicio());
        params.addValue("fechaFin", filtros.getFechaFin());
        params.addValue("fechaEspecifica", filtros.getFechaEspecifica());
    }

    // =================== MAPEO DE UBICACIÓN ===================

    private void mapearParametrosUbicacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Ubicación geográfica
        params.addValue("provincias", convertirLista(filtros.getProvincias(),String.class));
        params.addValue("municipios", convertirLista(filtros.getMunicipios(),String.class));
    }

    // =================== MAPEO DE EQUIPOS ===================

    private void mapearParametrosEquipos(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {

        // Tipos de dispositivos
        params.addValue("tiposDispositivos", convertirLista(filtros.getTiposDispositivos(),Integer.class));

        // Patrones para búsqueda con LIKE
        params.addValue("patronesEquipos", convertirLista(filtros.getPatronesEquipos(),String.class));


    }

    // =================== MAPEO DE INFRACCIONES ===================

    private void mapearParametrosInfracciones(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Tipos y estados
        params.addValue("tiposInfracciones", convertirLista(filtros.getTiposInfracciones(),Integer.class));
        params.addValue("estadosInfracciones", convertirLista(filtros.getEstadosInfracciones(), Integer.class));

        // Exportación SACIT
        params.addValue("exportadoSacit", filtros.getExportadoSacit());
    }

    // =================== MAPEO DE DOMINIOS Y VEHÍCULOS ===================

    private void mapearParametrosDominios(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {

        params.addValue("tiposVehiculos", convertirLista(filtros.getTipoVehiculo(),String.class));

        // Filtros de email
        params.addValue("tieneEmail", filtros.getTieneEmail());

    }


    // =================== MAPEO DE PAGINACIÓN ===================

    private void mapearPaginacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("limite", filtros.getLimiteMaximo());
        params.addValue("offset", filtros.calcularOffset());
    }

    // =================== MÉTODOS UTILITARIOS ===================

    /**
     * Convierte una lista a un arreglo para usar en PostgreSQL ANY()
     */
    @SuppressWarnings("unchecked")
    private static <T> T[] convertirLista(List<T> lista, Class<T> clazz) {
        return (lista != null && !lista.isEmpty())
                ? lista.toArray((T[]) java.lang.reflect.Array.newInstance(clazz, lista.size()))
                : null;
    }

}