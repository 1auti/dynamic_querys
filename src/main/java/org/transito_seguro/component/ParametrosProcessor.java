package org.transito_seguro.component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

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

    // =================== PATRONES GENÉRICOS (MANTENER COMPATIBILIDAD) ===================
    private static final Pattern FECHA_PATTERN = Pattern.compile("--\\s*FILTRO_FECHA\\s*--");
    private static final Pattern PROVINCIA_PATTERN = Pattern.compile("--\\s*FILTRO_PROVINCIA\\s*--");
    private static final Pattern MUNICIPIO_PATTERN = Pattern.compile("--\\s*FILTRO_MUNICIPIO\\s*--");
    private static final Pattern EQUIPO_PATTERN = Pattern.compile("--\\s*FILTRO_EQUIPO\\s*--");
    private static final Pattern INFRACCION_PATTERN = Pattern.compile("--\\s*FILTRO_INFRACCION\\s*--");
    private static final Pattern DOMINIO_PATTERN = Pattern.compile("--\\s*FILTRO_DOMINIO\\s*--");
    private static final Pattern PAGINACION_PATTERN = Pattern.compile("--\\s*FILTRO_PAGINACION\\s*--");
    private static final Pattern WHERE_PATTERN = Pattern.compile("WHERE\\s+", Pattern.CASE_INSENSITIVE);

    // =================== PATRONES ESPECÍFICOS (NUEVOS) ===================
    // Patrones específicos para DOMINIOS/PERSONAS JURÍDICAS
    private static final Pattern FECHA_DOMINIOS_PATTERN = Pattern.compile("--\\s*FILTRO_FECHA_DOMINIOS\\s*--");
    private static final Pattern PROVINCIA_TITULARES_PATTERN = Pattern.compile("--\\s*FILTRO_PROVINCIA_TITULARES\\s*--");
    private static final Pattern MUNICIPIO_TITULARES_PATTERN = Pattern.compile("--\\s*FILTRO_MUNICIPIO_TITULARES\\s*--");
    private static final Pattern DOMINIOS_ESPECIFICOS_PATTERN = Pattern.compile("--\\s*FILTRO_DOMINIOS_ESPECIFICOS\\s*--");

    // Patrones específicos para INFRACCIONES
    private static final Pattern FECHA_EXPORTACIONES_PATTERN = Pattern.compile("--\\s*FILTRO_FECHA_EXPORTACIONES\\s*--");
    private static final Pattern FECHA_INFRACCIONES_PATTERN = Pattern.compile("--\\s*FILTRO_FECHA_INFRACCIONES\\s*--");
    private static final Pattern PROVINCIA_CONCESIONES_PATTERN = Pattern.compile("--\\s*FILTRO_PROVINCIA_CONCESIONES\\s*--");
    private static final Pattern TIPO_INFRACCIONES_PATTERN = Pattern.compile("--\\s*FILTRO_TIPO_INFRACCIONES\\s*--");
    private static final Pattern EXPORTADO_SACIT_PATTERN = Pattern.compile("--\\s*FILTRO_EXPORTADO_SACIT\\s*--");
    private static final Pattern ESTADOS_INFRACCIONES_PATTERN = Pattern.compile("--\\s*FILTRO_ESTADOS_INFRACCIONES\\s*--");

    // Patrones específicos para EQUIPOS
    private static final Pattern EQUIPOS_SERIES_PATTERN = Pattern.compile("--\\s*FILTRO_EQUIPOS_SERIES\\s*--");
    private static final Pattern EQUIPOS_TIPOS_PATTERN = Pattern.compile("--\\s*FILTRO_EQUIPOS_TIPOS\\s*--");
    private static final Pattern EQUIPOS_LUGARES_PATTERN = Pattern.compile("--\\s*FILTRO_EQUIPOS_LUGARES\\s*--");

    /**
     * Procesa una query SQL y aplica todos los filtros dinámicos
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        String queryModificada = queryOriginal;

        log.debug("Procesando query. Tipo detectado: {}", detectarTipoQuery(queryOriginal));

        // *** NUEVO: Procesar filtros específicos primero ***
        queryModificada = procesarFiltrosEspecificos(queryModificada, filtros, parametros);

        // *** MANTENER: Procesar filtros genéricos para compatibilidad ***
        queryModificada = procesarFiltrosGenericos(queryModificada, filtros, parametros);

        // Procesar paginación (mantener igual)
        queryModificada = procesarPaginacion(queryModificada, filtros, parametros);

        // Asegurar que tenga WHERE (mantener igual)
        queryModificada = asegurarClausulaWhere(queryModificada);

        // Preparar metadata (mantener igual)
        prepararMetadata(metadata, filtros, parametros);

        log.debug("Query procesada exitosamente. Parámetros aplicados: {}", parametros.getParameterNames().length);

        return new QueryResult(queryModificada, parametros, metadata);
    }

    // =================== MÉTODOS ESPECÍFICOS (NUEVOS) ===================

    /**
     * Procesa filtros específicos (placeholders específicos)
     */
    private String procesarFiltrosEspecificos(String queryModificada, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        // Filtros específicos para dominios/personas jurídicas
        queryModificada = procesarFiltrosFechaDominios(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosProvinciaTitulares(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosMunicipioTitulares(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosDominiosEspecificos(queryModificada, filtros, parametros);

        // Filtros específicos para infracciones
        queryModificada = procesarFiltrosFechaExportaciones(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosFechaInfracciones(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosProvinciaConcesiones(queryModificada, filtros, parametros);
        queryModificada = procesarFiltrosTipoInfracciones(queryModificada, filtros, parametros);
        queryModificada = procesarFiltroExportadoSacit(queryModificada, filtros, parametros);
        queryModificada = procesarFiltroEstadosInfracciones(queryModificada, filtros, parametros);

        // Filtros específicos para equipos
        queryModificada = procesarFiltroEquiposSeries(queryModificada, filtros, parametros);
        queryModificada = procesarFiltroEquiposTipos(queryModificada, filtros, parametros);
        queryModificada = procesarFiltroEquiposLugares(queryModificada, filtros, parametros);

        return queryModificada;
    }

    /**
     * Filtros de fecha específicos para tabla dominios (d.fecha_alta)
     */
    private String procesarFiltrosFechaDominios(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesFecha = new StringBuilder();

        if (filtros.debeUsarFechaEspecifica()) {
            condicionesFecha.append(" AND DATE(d.fecha_alta) = DATE(:fechaEspecificaDominios)");
            parametros.addValue("fechaEspecificaDominios", filtros.getFechaEspecifica());
        } else {
            if (filtros.getFechaInicio() != null) {
                condicionesFecha.append(" AND d.fecha_alta >= :fechaInicioDominios");
                parametros.addValue("fechaInicioDominios", filtros.getFechaInicio());
            }
            if (filtros.getFechaFin() != null) {
                condicionesFecha.append(" AND d.fecha_alta <= :fechaFinDominios");
                parametros.addValue("fechaFinDominios", filtros.getFechaFin());
            }
        }

        return reemplazarPlaceholder(query, FECHA_DOMINIOS_PATTERN, condicionesFecha.toString());
    }

    /**
     * Filtros de fecha específicos para tabla exportaciones (elh.fecha_alta)
     */
    private String procesarFiltrosFechaExportaciones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesFecha = new StringBuilder();

        if (filtros.debeUsarFechaEspecifica()) {
            condicionesFecha.append(" AND DATE(elh.fecha_alta) = DATE(:fechaEspecificaExportaciones)");
            parametros.addValue("fechaEspecificaExportaciones", filtros.getFechaEspecifica());
        } else {
            if (filtros.getFechaInicio() != null) {
                condicionesFecha.append(" AND elh.fecha_alta >= :fechaInicioExportaciones");
                parametros.addValue("fechaInicioExportaciones", filtros.getFechaInicio());
            }
            if (filtros.getFechaFin() != null) {
                condicionesFecha.append(" AND elh.fecha_alta <= :fechaFinExportaciones");
                parametros.addValue("fechaFinExportaciones", filtros.getFechaFin());
            }
        }

        return reemplazarPlaceholder(query, FECHA_EXPORTACIONES_PATTERN, condicionesFecha.toString());
    }

    /**
     * Filtros de fecha específicos para tabla infraccion (i.fecha_infraccion)
     */
    private String procesarFiltrosFechaInfracciones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesFecha = new StringBuilder();

        if (filtros.debeUsarFechaEspecifica()) {
            condicionesFecha.append(" AND DATE(i.fecha_infraccion) = DATE(:fechaEspecificaInfracciones)");
            parametros.addValue("fechaEspecificaInfracciones", filtros.getFechaEspecifica());
        } else {
            if (filtros.getFechaInicio() != null) {
                condicionesFecha.append(" AND i.fecha_infraccion >= :fechaInicioInfracciones");
                parametros.addValue("fechaInicioInfracciones", filtros.getFechaInicio());
            }
            if (filtros.getFechaFin() != null) {
                condicionesFecha.append(" AND i.fecha_infraccion <= :fechaFinInfracciones");
                parametros.addValue("fechaFinInfracciones", filtros.getFechaFin());
            }
        }

        return reemplazarPlaceholder(query, FECHA_INFRACCIONES_PATTERN, condicionesFecha.toString());
    }

    /**
     * Filtros de provincia específicos para tabla titulares (dt.provincia)
     */
    private String procesarFiltrosProvinciaTitulares(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesProvincias = new StringBuilder();

        if (!filtros.debeConsultarTodasLasBDS() && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            condicionesProvincias.append(" AND dt.provincia IN (:provinciasTitulares)");
            parametros.addValue("provinciasTitulares", filtros.getBaseDatos());
        }

        return reemplazarPlaceholder(query, PROVINCIA_TITULARES_PATTERN, condicionesProvincias.toString());
    }

    /**
     * Filtros de provincia específicos para tabla concesiones (c.descripcion)
     */
    private String procesarFiltrosProvinciaConcesiones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesProvincias = new StringBuilder();

        if (!filtros.debeConsultarTodasLasBDS() && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            condicionesProvincias.append(" AND c.descripcion IN (:provinciasConcesiones)");
            parametros.addValue("provinciasConcesiones", filtros.getBaseDatos());
        }

        return reemplazarPlaceholder(query, PROVINCIA_CONCESIONES_PATTERN, condicionesProvincias.toString());
    }

    /**
     * Filtros de municipio específicos para tabla titulares (dt.localidad)
     */
    private String procesarFiltrosMunicipioTitulares(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesMunicipios = new StringBuilder();

        if (filtros.getMunicipios() != null && !filtros.getMunicipios().isEmpty()) {
            condicionesMunicipios.append(" AND dt.localidad IN (:municipiosTitulares)");
            parametros.addValue("municipiosTitulares", filtros.getMunicipios());
        }

        return reemplazarPlaceholder(query, MUNICIPIO_TITULARES_PATTERN, condicionesMunicipios.toString());
    }

    /**
     * Filtros específicos para dominios y datos de titulares
     */
    private String procesarFiltrosDominiosEspecificos(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesDominios = new StringBuilder();

        if (filtros.getDominios() != null && !filtros.getDominios().isEmpty()) {
            condicionesDominios.append(" AND d.dominio IN (:dominiosEspecificos)");
            parametros.addValue("dominiosEspecificos", filtros.getDominios());
        }

        if (filtros.getNumerosDocumentos() != null && !filtros.getNumerosDocumentos().isEmpty()) {
            condicionesDominios.append(" AND dt.numero_documento IN (:numerosDocumentosEspecificos)");
            parametros.addValue("numerosDocumentosEspecificos", filtros.getNumerosDocumentos());
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
                    .append("dt.propietario ILIKE :textoBusquedaEspecifica OR ")
                    .append("dt.numero_documento LIKE :textoBusquedaExactaEspecifica OR ")
                    .append("d.dominio LIKE :textoBusquedaExactaEspecifica")
                    .append(")");
            parametros.addValue("textoBusquedaEspecifica", "%" + filtros.getTextoBusqueda() + "%");
            parametros.addValue("textoBusquedaExactaEspecifica", filtros.getTextoBusqueda());
        }

        if (filtros.getMarcas() != null && !filtros.getMarcas().isEmpty()) {
            condicionesDominios.append(" AND d.marca IN (:marcasEspecificas)");
            parametros.addValue("marcasEspecificas", filtros.getMarcas());
        }

        if (filtros.getModelos() != null && !filtros.getModelos().isEmpty()) {
            condicionesDominios.append(" AND d.modelo IN (:modelosEspecificos)");
            parametros.addValue("modelosEspecificos", filtros.getModelos());
        }

        if (filtros.getSoloPersonasJuridicas() != null && filtros.getSoloPersonasJuridicas()) {
            condicionesDominios.append(" AND dt.sexo = 'J'");
        }

        if (filtros.getSoloPersonasFisicas() != null && filtros.getSoloPersonasFisicas()) {
            condicionesDominios.append(" AND dt.sexo != 'J'");
        }

        return reemplazarPlaceholder(query, DOMINIOS_ESPECIFICOS_PATTERN, condicionesDominios.toString());
    }

    /**
     * Filtros de tipo de infracciones específicos (ti.id)
     */
    private String procesarFiltrosTipoInfracciones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesTipos = new StringBuilder();

        if (filtros.getTiposInfracciones() != null && !filtros.getTiposInfracciones().isEmpty()) {
            condicionesTipos.append(" AND ti.id IN (:tiposInfraccionesEspecificos)");
            parametros.addValue("tiposInfraccionesEspecificos", filtros.getTiposInfracciones());
        }

        return reemplazarPlaceholder(query, TIPO_INFRACCIONES_PATTERN, condicionesTipos.toString());
    }

    /**
     * Filtro específico para exportado a SACIT
     */
    private String procesarFiltroExportadoSacit(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionSacit = new StringBuilder();

        if (filtros.getExportadoSacit() != null) {
            condicionSacit.append(" AND elh.exporta_sacit = :exportadoSacitEspecifico");
            parametros.addValue("exportadoSacitEspecifico", filtros.getExportadoSacit());
        }

        return reemplazarPlaceholder(query, EXPORTADO_SACIT_PATTERN, condicionSacit.toString());
    }

    /**
     * Filtros de estados de infracciones específicos (i.id_estado)
     */
    private String procesarFiltroEstadosInfracciones(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesEstados = new StringBuilder();

        if (filtros.getEstadosInfracciones() != null && !filtros.getEstadosInfracciones().isEmpty()) {
            condicionesEstados.append(" AND i.id_estado IN (:estadosInfraccionesEspecificos)");
            parametros.addValue("estadosInfraccionesEspecificos", filtros.getEstadosInfracciones());
        }

        return reemplazarPlaceholder(query, ESTADOS_INFRACCIONES_PATTERN, condicionesEstados.toString());
    }

    /**
     * Filtros específicos para series de equipos (pc.serie_equipo)
     */
    private String procesarFiltroEquiposSeries(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesSeries = new StringBuilder();

        if (filtros.getSeriesEquipos() != null && !filtros.getSeriesEquipos().isEmpty()) {
            condicionesSeries.append(" AND pc.serie_equipo IN (:seriesEquiposEspecificos)");
            parametros.addValue("seriesEquiposEspecificos", filtros.getSeriesEquipos());
        }

        return reemplazarPlaceholder(query, EQUIPOS_SERIES_PATTERN, condicionesSeries.toString());
    }

    /**
     * Filtros específicos para tipos de equipos (pc.id_tipo_dispositivo)
     */
    private String procesarFiltroEquiposTipos(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesTipos = new StringBuilder();

        if (filtros.getTiposDispositivos() != null && !filtros.getTiposDispositivos().isEmpty()) {
            condicionesTipos.append(" AND pc.id_tipo_dispositivo IN (:tiposDispositivosEspecificos)");
            parametros.addValue("tiposDispositivosEspecificos", filtros.getTiposDispositivos());
        }

        return reemplazarPlaceholder(query, EQUIPOS_TIPOS_PATTERN, condicionesTipos.toString());
    }

    /**
     * Filtros específicos para lugares de equipos (pc.lugar)
     */
    private String procesarFiltroEquiposLugares(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesLugares = new StringBuilder();

        if (filtros.getLugares() != null && !filtros.getLugares().isEmpty()) {
            condicionesLugares.append(" AND pc.lugar IN (:lugaresEspecificos)");
            parametros.addValue("lugaresEspecificos", filtros.getLugares());
        }

        return reemplazarPlaceholder(query, EQUIPOS_LUGARES_PATTERN, condicionesLugares.toString());
    }

    /**
     * Método específico para reemplazar placeholders
     */
    private String reemplazarPlaceholder(String query, Pattern pattern, String condicion) {
        if (StringUtils.hasText(condicion)) {
            return pattern.matcher(query).replaceAll(condicion);
        }
        return pattern.matcher(query).replaceAll(""); // Eliminar placeholder vacío
    }

    // =================== MÉTODOS GENÉRICOS (COMPATIBILIDAD) ===================

    /**
     * Procesa filtros genéricos para mantener compatibilidad
     */
    private String procesarFiltrosGenericos(String queryModificada, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        // Solo aplicar filtros genéricos si no se aplicaron específicos
        if (!tieneplaceholdersEspecificos(queryModificada)) {
            log.debug("Usando filtros genéricos para compatibilidad");

            // Procesar filtros genéricos (TUS MÉTODOS ORIGINALES)
            queryModificada = procesarFiltrosFechaGenerico(queryModificada, filtros, parametros);
            queryModificada = procesarFiltrosProvinciasGenerico(queryModificada, filtros, parametros);
            queryModificada = procesarFiltrosMunicipiosGenerico(queryModificada, filtros, parametros);
            queryModificada = procesarFiltrosEquiposGenerico(queryModificada, filtros, parametros);
            queryModificada = procesarFiltrosInfraccionesGenerico(queryModificada, filtros, parametros);
            queryModificada = procesarFiltrosDominiosGenerico(queryModificada, filtros, parametros);
        } else {
            log.debug("Usando filtros específicos");
        }

        return queryModificada;
    }

    /**
     * Verifica si la query usa placeholders específicos
     */
    private boolean tieneplaceholdersEspecificos(String query) {
        return query.contains("FILTRO_FECHA_DOMINIOS") ||
                query.contains("FILTRO_FECHA_EXPORTACIONES") ||
                query.contains("FILTRO_FECHA_INFRACCIONES") ||
                query.contains("FILTRO_PROVINCIA_TITULARES") ||
                query.contains("FILTRO_PROVINCIA_CONCESIONES") ||
                query.contains("FILTRO_DOMINIOS_ESPECIFICOS") ||
                query.contains("FILTRO_TIPO_INFRACCIONES") ||
                query.contains("FILTRO_EQUIPOS_SERIES");
    }

    /**
     * Detecta tipo de query para logging
     */
    private String detectarTipoQuery(String query) {
        String queryUpper = query.toUpperCase();
        if (queryUpper.contains("DOMINIOS D") && queryUpper.contains("DOMINIO_TITULARES DT")) {
            return "personas_juridicas";
        } else if (queryUpper.contains("EXPORTACIONES_LOTE_HEADER ELH")) {
            return "infracciones_exportaciones";
        } else if (queryUpper.contains("FROM INFRACCION I")) {
            return "infracciones_directas";
        }
        return "generico";
    }

    // =================== MÉTODOS GENÉRICOS ORIGINALES (MANTENIDOS) ===================

    /**
     * Procesa filtros relacionados con fechas (GENÉRICO)
     */
    private String procesarFiltrosFechaGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
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
     * Procesa filtros de provincias/bases de datos (GENÉRICO)
     */
    private String procesarFiltrosProvinciasGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesProvincias = new StringBuilder();

        if (!filtros.debeConsultarTodasLasBDS() && filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            condicionesProvincias.append(" AND c.descripcion IN (:provincias)");
            parametros.addValue("provincias", filtros.getBaseDatos());
        }

        return reemplazarOAgregar(query, PROVINCIA_PATTERN, condicionesProvincias.toString());
    }

    /**
     * Procesa filtros de municipios (GENÉRICO)
     */
    private String procesarFiltrosMunicipiosGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
        StringBuilder condicionesMunicipios = new StringBuilder();

        if (filtros.getMunicipios() != null && !filtros.getMunicipios().isEmpty()) {
            condicionesMunicipios.append(" AND c.descripcion IN (:municipios)");
            parametros.addValue("municipios", filtros.getMunicipios());
        }

        return reemplazarOAgregar(query, MUNICIPIO_PATTERN, condicionesMunicipios.toString());
    }

    /**
     * Procesa filtros de equipos (GENÉRICO)
     */
    private String procesarFiltrosEquiposGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
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
     * Procesa filtros de infracciones (GENÉRICO)
     */
    private String procesarFiltrosInfraccionesGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
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
     * Procesa filtros de dominios y vehículos (GENÉRICO)
     */
    private String procesarFiltrosDominiosGenerico(String query, ParametrosFiltrosDTO filtros, MapSqlParameterSource parametros) {
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

    // =================== MÉTODOS UTILITARIOS (MANTENIDOS) ===================

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