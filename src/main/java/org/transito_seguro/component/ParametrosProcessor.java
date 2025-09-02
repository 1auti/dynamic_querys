package org.transito_seguro.component;

import lombok.Getter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

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

    // Patrones para identificar placeholders en las queries
    private static final Pattern FECHA_PATTERN = Pattern.compile("--\\s*FILTRO_FECHA\\s*--");
    private static final Pattern PROVINCIA_PATTERN = Pattern.compile("--\\s*FILTRO_PROVINCIA\\s*--");
    private static final Pattern MUNICIPIO_PATTERN = Pattern.compile("--\\s*FILTRO_MUNICIPIO\\s*--");
    private static final Pattern EQUIPO_PATTERN = Pattern.compile("--\\s*FILTRO_EQUIPO\\s*--");
    private static final Pattern INFRACCION_PATTERN = Pattern.compile("--\\s*FILTRO_INFRACCION\\s*--");
    private static final Pattern DOMINIO_PATTERN = Pattern.compile("--\\s*FILTRO_DOMINIO\\s*--");
    private static final Pattern PAGINACION_PATTERN = Pattern.compile("--\\s*FILTRO_PAGINACION\\s*--");
    private static final Pattern WHERE_PATTERN = Pattern.compile("WHERE\\s+", Pattern.CASE_INSENSITIVE);

    /**
     * Procesa una query SQL y aplica todos los filtros dinámicos
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        String queryModificada = queryOriginal;

        // 1. Procesar filtros de fecha
        queryModificada = procesarFiltrosFecha(queryModificada, filtros, parametros);

        // 2. Procesar filtros de provincia/bases de datos
        queryModificada = procesarFiltrosProvincias(queryModificada, filtros, parametros);

        // 3. Procesar filtros de municipios
        queryModificada = procesarFiltrosMunicipios(queryModificada, filtros, parametros);

        // 4. Procesar filtros de equipos
        queryModificada = procesarFiltrosEquipos(queryModificada, filtros, parametros);

        // 5. Procesar filtros de infracciones
        queryModificada = procesarFiltrosInfracciones(queryModificada, filtros, parametros);

        // 6. Procesar filtros de dominios/vehículos
        queryModificada = procesarFiltrosDominios(queryModificada, filtros, parametros);

        // 7. Procesar paginación
        queryModificada = procesarPaginacion(queryModificada, filtros, parametros);

        // 8. Asegurar que tenga WHERE
        queryModificada = asegurarClausulaWhere(queryModificada);

        // 9. Preparar metadata
        prepararMetadata(metadata, filtros, parametros);

        return new QueryResult(queryModificada, parametros, metadata);
    }

    /**
     * Procesa filtros relacionados con fechas
     */
    private String procesarFiltrosFecha(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesFecha = new StringBuilder();

        if (filtros.debeUsarFechaEspecifica()) {
            condicionesFecha.append(" AND DATE(elh.fecha_alta) = DATE(:fechaEspecifica)");
            parametros.addValue("fechaEspecifica", filtros.getFechaEspecifica());
        } else {
            if (filtros.getFechaInicio() != null) {
                condicionesFecha.append(" AND elh.fecha_alta >= :fechaInicio");
                parametros.addValue("fechaInicio", filtros.getFechaInicio());
            }
            if (filtros.getFechaFin() != null) {
                condicionesFecha.append(" AND elh.fecha_alta <= :fechaFin");
                parametros.addValue("fechaFin", filtros.getFechaFin());
            }
        }

        return reemplazarOAgregar(query, FECHA_PATTERN, condicionesFecha.toString());
    }

    /**
     * Procesa filtros de provincias/bases de datos
     */
    private String procesarFiltrosProvincias(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesProvincias = new StringBuilder();

        if (!filtros.debeConsultarTodasLasBDS() && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            condicionesProvincias.append(" AND c.descripcion IN (:provincias)");
            parametros.addValue("provincias", filtros.getBaseDatos());
        }

        return reemplazarOAgregar(query, PROVINCIA_PATTERN, condicionesProvincias.toString());
    }

    /**
     * Procesa filtros de municipios
     */
    private String procesarFiltrosMunicipios(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesMunicipios = new StringBuilder();

        if (filtros.getMunicipios() != null && !filtros.getMunicipios().isEmpty()) {
            condicionesMunicipios.append(" AND c.descripcion IN (:municipios)");
            parametros.addValue("municipios", filtros.getMunicipios());
        }

        return reemplazarOAgregar(query, MUNICIPIO_PATTERN, condicionesMunicipios.toString());
    }

    /**
     * Procesa filtros de equipos
     */
    private String procesarFiltrosEquipos(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesEquipos = new StringBuilder();

        if (filtros.getSeriesEquipos() != null && !filtros.getSeriesEquipos().isEmpty()) {
            condicionesEquipos.append(" AND pc.serie_equipo IN (:seriesEquipos)");
            parametros.addValue("seriesEquipos", filtros.getSeriesEquipos());
        }

        if (filtros.getTiposDispositivos() != null && !filtros.getTiposDispositivos().isEmpty()) {
            condicionesEquipos.append(" AND pc.id_tipo_dispositivo IN (:tiposDispositivos)");
            parametros.addValue("tiposDispositivos", filtros.getTiposDispositivos());
        }

        if (filtros.getLugares() != null && !filtros.getLugares().isEmpty()) {
            condicionesEquipos.append(" AND pc.lugar IN (:lugares)");
            parametros.addValue("lugares", filtros.getLugares());
        }

        return reemplazarOAgregar(query, EQUIPO_PATTERN, condicionesEquipos.toString());
    }

    /**
     * Procesa filtros de infracciones
     */
    private String procesarFiltrosInfracciones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesInfracciones = new StringBuilder();

        if (filtros.getTiposInfracciones() != null && !filtros.getTiposInfracciones().isEmpty()) {
            condicionesInfracciones.append(" AND ti.id IN (:tiposInfracciones)");
            parametros.addValue("tiposInfracciones", filtros.getTiposInfracciones());
        }

        if (filtros.getEstadosInfracciones() != null && !filtros.getEstadosInfracciones().isEmpty()) {
            condicionesInfracciones.append(" AND i.id_estado IN (:estadosInfracciones)");
            parametros.addValue("estadosInfracciones", filtros.getEstadosInfracciones());
        }

        if (filtros.getExportadoSacit() != null) {
            condicionesInfracciones.append(" AND elh.exporta_sacit = :exportadoSacit");
            parametros.addValue("exportadoSacit", filtros.getExportadoSacit());
        }

        if (filtros.getMontoMinimo() != null) {
            condicionesInfracciones.append(" AND i.monto >= :montoMinimo");
            parametros.addValue("montoMinimo", filtros.getMontoMinimo());
        }

        if (filtros.getMontoMaximo() != null) {
            condicionesInfracciones.append(" AND i.monto <= :montoMaximo");
            parametros.addValue("montoMaximo", filtros.getMontoMaximo());
        }

        if (filtros.getTieneImagenes() != null) {
            if (filtros.getTieneImagenes()) {
                condicionesInfracciones.append(" AND i.packedfile IS NOT NULL");
            } else {
                condicionesInfracciones.append(" AND i.packedfile IS NULL");
            }
        }

        return reemplazarOAgregar(query, INFRACCION_PATTERN, condicionesInfracciones.toString());
    }

    /**
     * Procesa filtros de dominios y vehículos
     */
    private String procesarFiltrosDominios(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesDominios = new StringBuilder();

        if (filtros.getDominios() != null && !filtros.getDominios().isEmpty()) {
            condicionesDominios.append(" AND d.dominio IN (:dominios)");
            parametros.addValue("dominios", filtros.getDominios());
        }

        if (filtros.getMarcas() != null && !filtros.getMarcas().isEmpty()) {
            condicionesDominios.append(" AND d.marca IN (:marcas)");
            parametros.addValue("marcas", filtros.getMarcas());
        }

        if (filtros.getModelos() != null && !filtros.getModelos().isEmpty()) {
            condicionesDominios.append(" AND d.modelo IN (:modelos)");
            parametros.addValue("modelos", filtros.getModelos());
        }

        if (filtros.getTiposVehiculos() != null && !filtros.getTiposVehiculos().isEmpty()) {
            condicionesDominios.append(" AND d.tipo_vehiculo IN (:tiposVehiculos)");
            parametros.addValue("tiposVehiculos", filtros.getTiposVehiculos());
        }

        if (filtros.getNumerosDocumentos() != null && !filtros.getNumerosDocumentos().isEmpty()) {
            condicionesDominios.append(" AND dt.numero_documento IN (:numerosDocumentos)");
            parametros.addValue("numerosDocumentos", filtros.getNumerosDocumentos());
        }

        if (filtros.getSoloPersonasJuridicas() != null && filtros.getSoloPersonasJuridicas()) {
            condicionesDominios.append(" AND dt.sexo = 'J'");
        }

        if (filtros.getSoloPersonasFisicas() != null && filtros.getSoloPersonasFisicas()) {
            condicionesDominios.append(" AND dt.sexo != 'J'");
        }

        if (filtros.getTieneEmail() != null) {
            if (filtros.getTieneEmail()) {
                condicionesDominios.append(" AND dt.email IS NOT NULL AND dt.email != ''");
            } else {
                condicionesDominios.append(" AND (dt.email IS NULL OR dt.email = '')");
            }
        }

        if (StringUtils.hasText(filtros.getTextoBusqueda())) {
            condicionesDominios.append(" AND (")
                    .append("dt.propietario ILIKE :textoBusqueda OR ")
                    .append("dt.numero_documento LIKE :textoBusquedaExacta OR ")
                    .append("d.dominio LIKE :textoBusquedaExacta")
                    .append(")");
            parametros.addValue("textoBusqueda", "%" + filtros.getTextoBusqueda() + "%");
            parametros.addValue("textoBusquedaExacta", filtros.getTextoBusqueda());
        }

        return reemplazarOAgregar(query, DOMINIO_PATTERN, condicionesDominios.toString());
    }

    /**
     * Procesa paginación y ordenamiento
     */
    private String procesarPaginacion(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder paginacionClause = new StringBuilder();

        // Ordenamiento
        if (StringUtils.hasText(filtros.getOrdenarPor())) {
            paginacionClause.append(" ORDER BY ")
                    .append(filtros.getOrdenarPor())
                    .append(" ")
                    .append(filtros.getDireccionOrdenamiento());
        }

        // Límite y offset
        if (filtros.getLimiteMaximo() != null || filtros.getTamanoPagina() != null) {
            int limite = filtros.getLimiteMaximo() != null ?
                    filtros.getLimiteMaximo() : filtros.obtenerTamanoPaginaSeguro();

            paginacionClause.append(" LIMIT :limite");
            parametros.addValue("limite", limite);

            if (filtros.getPagina() != null && filtros.getPagina() > 0) {
                paginacionClause.append(" OFFSET :offset");
                parametros.addValue("offset", filtros.calcularOffset());
            }
        }

        return reemplazarOAgregar(query, PAGINACION_PATTERN, paginacionClause.toString());
    }

    /**
     * Método utilitario para reemplazar placeholders o agregar condiciones
     */
    private String reemplazarOAgregar(String query, Pattern pattern, String condicion) {
        if (StringUtils.hasText(condicion)) {
            if (pattern.matcher(query).find()) {
                return pattern.matcher(query).replaceAll(condicion);
            } else {
                return agregarCondicionAlWhere(query, condicion);
            }
        }
        return query;
    }

    /**
     * Asegura que la query tenga una cláusula WHERE
     */
    private String asegurarClausulaWhere(String query) {
        if (!WHERE_PATTERN.matcher(query).find() && !query.toUpperCase().contains("WHERE")) {
            String upperQuery = query.toUpperCase();
            int insertPos = encontrarPosicionInsercion(query, upperQuery);
            query = query.substring(0, insertPos).trim() + " WHERE 1=1 " + query.substring(insertPos);
        }
        return query;
    }

    /**
     * Encuentra la posición donde insertar WHERE
     */
    private int encontrarPosicionInsercion(String query, String upperQuery) {
        int insertPos = query.length();
        int groupByPos = upperQuery.indexOf("GROUP BY");
        int orderByPos = upperQuery.indexOf("ORDER BY");
        int limitPos = upperQuery.indexOf("LIMIT");

        if (groupByPos > 0) insertPos = Math.min(insertPos, groupByPos);
        if (orderByPos > 0) insertPos = Math.min(insertPos, orderByPos);
        if (limitPos > 0) insertPos = Math.min(insertPos, limitPos);

        return insertPos;
    }

    /**
     * Agrega una condición a una query que ya tiene WHERE
     */
    private String agregarCondicionAlWhere(String query, String condicion) {
        String upperQuery = query.toUpperCase();
        int wherePos = upperQuery.lastIndexOf("WHERE");

        if (wherePos > 0) {
            int insertPos = encontrarFinWhere(query, upperQuery, wherePos);
            query = query.substring(0, insertPos).trim() + condicion + " " + query.substring(insertPos);
        }

        return query;
    }

    /**
     * Encuentra el final de la cláusula WHERE
     */
    private int encontrarFinWhere(String query, String upperQuery, int wherePos) {
        int insertPos = query.length();
        int groupByPos = upperQuery.indexOf("GROUP BY", wherePos);
        int orderByPos = upperQuery.indexOf("ORDER BY", wherePos);
        int limitPos = upperQuery.indexOf("LIMIT", wherePos);

        if (groupByPos > 0) insertPos = Math.min(insertPos, groupByPos);
        if (orderByPos > 0) insertPos = Math.min(insertPos, orderByPos);
        if (limitPos > 0) insertPos = Math.min(insertPos, limitPos);

        return insertPos;
    }

    /**
     * Prepara metadata del resultado
     */
    private void prepararMetadata(Map<String, Object> metadata, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        metadata.put("filtrosAplicados", parametros.getParameterNames().length);
        metadata.put("tieneFiltroFecha", filtros.debeUsarFechaEspecifica() || filtros.tieneRangoFechas());
        metadata.put("tieneFiltroProvincias", filtros.tieneFiltrosUbicacion());
        metadata.put("tieneFiltroEquipos", filtros.tieneFiltrosEquipos());
        metadata.put("tieneFiltroInfracciones", filtros.tieneFiltrosInfracciones());
        metadata.put("paginaActual", filtros.getPagina());
        metadata.put("tamanoPagina", filtros.obtenerTamanoPaginaSeguro());
        metadata.put("consultarTodasBDS", filtros.debeConsultarTodasLasBDS());
    }

    /**
     * Método público para generar ORDER BY
     */
    public String generarOrderBy(String campo, String direccion) {
        if (!StringUtils.hasText(campo)) return "";
        return String.format(" ORDER BY %s %s ", campo, StringUtils.hasText(direccion) ? direccion : "ASC");
    }

    /**
     * Método público para generar LIMIT/OFFSET
     */
    public String generarLimit(Integer limite, Integer offset) {
        if (limite != null && limite > 0) {
            StringBuilder limitClause = new StringBuilder(" LIMIT ").append(limite);
            if (offset != null && offset > 0) {
                limitClause.append(" OFFSET ").append(offset);
            }
            return limitClause.toString();
        }
        return "";
    }
}