package org.transito_seguro.component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
     * Método principal - SOLO CURSOR (elimina lógica de OFFSET)
     */
    public QueryResult procesarQuery(String queryOriginal, ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource parametros = new MapSqlParameterSource();
        Map<String, Object> metadata = new HashMap<>();

        // Mapear parámetros de filtros tradicionales
        mapearParametroFechas(filtros, parametros);
        mapearParametrosUbicacion(filtros, parametros);
        mapearParametrosEquipos(filtros, parametros);
        mapearParametrosInfracciones(filtros, parametros);
        mapearParametrosDominios(filtros, parametros);
        mapearParametrosAdicionales(filtros, parametros);

        // =============== PROCESAMIENTO DE CURSOR ===============
        String queryFinal = procesarQueryConCursor(queryOriginal, filtros, parametros, metadata);

        log.debug("Query procesada con CURSOR: {}", filtros.getInfoPaginacion());
        log.debug("Parámetros mapeados: {}", parametros.getParameterNames().length);

        return new QueryResult(queryFinal, parametros, metadata);
    }

    /**
     * Procesa query aplicando cursor (reemplaza OFFSET/LIMIT tradicional)
     */
    private String procesarQueryConCursor(String queryOriginal, ParametrosFiltrosDTO filtros,
                                          MapSqlParameterSource parametros, Map<String, Object> metadata) {

        String tipoCursor = filtros.getTipoCursor();
        boolean esHaciaAtras = filtros.esHaciaAtras();

        // Mapear parámetros específicos del cursor
        mapearParametrosCursor(filtros, parametros);

        // Modificar query
        String queryConCursor = aplicarModificacionesCursor(queryOriginal, filtros);

        // Metadata para debugging
        metadata.put("tipo_cursor", tipoCursor);
        metadata.put("direccion", filtros.getDireccion());
        metadata.put("es_primera_pagina", filtros.esPrimeraPagina());
        metadata.put("page_size", filtros.getPageSizeEfectivo());

        return queryConCursor;
    }

    /**
     * Mapea parámetros específicos del cursor
     */
    private void mapearParametrosCursor(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        // Límite SQL (pageSize + 1 para detectar hasNext)
        params.addValue("limiteCursor", filtros.getLimiteSql(), Types.INTEGER);

        // Parámetros según tipo de cursor
        String tipoCursor = filtros.getTipoCursor();

        if (filtros.esPrimeraPagina()) {
            // Primera página - no agregar filtros de cursor
            params.addValue("cursorId", null, Types.BIGINT);
            params.addValue("cursorFecha", null, Types.TIMESTAMP);
        } else {
            // Páginas siguientes - agregar filtros de cursor
            switch (tipoCursor) {
                case "id":
                    params.addValue("cursorId", filtros.getCursorId(), Types.BIGINT);
                    params.addValue("cursorFecha", null, Types.TIMESTAMP);
                    break;

                case "fecha":
                    params.addValue("cursorFecha", parsearFecha(filtros.getCursorFecha()), Types.TIMESTAMP);
                    params.addValue("cursorId", null, Types.BIGINT);
                    break;

                case "fecha_id":
                    params.addValue("cursorFecha", parsearFecha(filtros.getCursorFecha()), Types.TIMESTAMP);
                    params.addValue("cursorId", filtros.getCursorId(), Types.BIGINT);
                    break;

                default:
                    log.warn("Tipo de cursor no soportado: {}, usando 'id'", tipoCursor);
                    params.addValue("cursorId", filtros.getCursorId(), Types.BIGINT);
                    params.addValue("cursorFecha", null, Types.TIMESTAMP);
                    break;
            }
        }
    }

    /**
     * Aplica todas las modificaciones de cursor a la query
     */
    private String aplicarModificacionesCursor(String queryOriginal, ParametrosFiltrosDTO filtros) {

        // 1. Asegurar ORDER BY correcto
        String queryConOrden = asegurarOrdenamiento(queryOriginal, filtros);

        // 2. Aplicar filtro de cursor si no es primera página
        String queryConFiltro = filtros.esPrimeraPagina() ?
                queryConOrden : aplicarFiltroCursor(queryConOrden, filtros);

        // 3. Reemplazar LIMIT/OFFSET con límite de cursor
        String queryFinal = aplicarLimiteCursor(queryConFiltro);

        log.debug("Query original modificada para cursor tipo: {}", filtros.getTipoCursor());

        return queryFinal;
    }

    /**
     * Asegura ORDER BY apropiado para el cursor
     */
    private String asegurarOrdenamiento(String query, ParametrosFiltrosDTO filtros) {
        String tipoCursor = filtros.getTipoCursor();
        boolean esHaciaAtras = filtros.esHaciaAtras();

        // Determinar dirección de orden
        String direccionOrden = esHaciaAtras ? "ASC" : "DESC";
        String direccionOrdenSecundaria = esHaciaAtras ? "ASC" : "DESC";

        if (query.toUpperCase().contains("ORDER BY")) {
            // Ya tiene ORDER BY - verificar que sea compatible con cursor
            return modificarOrdenExistente(query, tipoCursor, direccionOrden, direccionOrdenSecundaria);
        } else {
            // Agregar ORDER BY completo
            return agregarOrdenamiento(query, tipoCursor, direccionOrden, direccionOrdenSecundaria);
        }
    }

    /**
     * Modifica ORDER BY existente para ser compatible con cursor
     */
    private String modificarOrdenExistente(String query, String tipoCursor, String direccion, String direccionSecundaria) {
        // Para simplicidad, mantener ORDER BY existente y agregar campos de cursor si faltan
        String ordenAdicional = "";

        switch (tipoCursor) {
            case "id":
                if (!contieneOrdenPor(query, "id")) {
                    ordenAdicional = ", id " + direccion;
                }
                break;
            case "fecha":
                if (!contieneOrdenPor(query, "fecha_alta")) {
                    ordenAdicional = ", fecha_alta " + direccion;
                }
                break;
            case "fecha_id":
                if (!contieneOrdenPor(query, "fecha_alta")) {
                    ordenAdicional = ", fecha_alta " + direccion;
                }
                if (!contieneOrdenPor(query, "id")) {
                    ordenAdicional += ", id " + direccionSecundaria;
                }
                break;
        }

        // Insertar antes de LIMIT si existe
        if (query.toUpperCase().contains("LIMIT")) {
            return query.replaceFirst("(?i)\\s+LIMIT", ordenAdicional + " LIMIT");
        } else {
            return query + ordenAdicional;
        }
    }

    /**
     * Agrega ORDER BY completo cuando no existe
     */
    private String agregarOrdenamiento(String query, String tipoCursor, String direccion, String direccionSecundaria) {
        String orden;

        switch (tipoCursor) {
            case "id":
                orden = "ORDER BY id " + direccion;
                break;
            case "fecha":
                orden = "ORDER BY fecha_alta " + direccion;
                break;
            case "fecha_id":
                orden = "ORDER BY fecha_alta " + direccion + ", id " + direccionSecundaria;
                break;
            default:
                orden = "ORDER BY id " + direccion; // Fallback
                break;
        }

        // Insertar antes de LIMIT si existe
        if (query.toUpperCase().contains("LIMIT")) {
            return query.replaceFirst("(?i)\\s+LIMIT", " " + orden + " LIMIT");
        } else {
            return query + " " + orden;
        }
    }

    /**
     * Aplica filtro WHERE para el cursor
     */
    private String aplicarFiltroCursor(String query, ParametrosFiltrosDTO filtros) {
        String tipoCursor = filtros.getTipoCursor();
        boolean esHaciaAtras = filtros.esHaciaAtras();
        String operador = esHaciaAtras ? "<" : ">";

        String filtro;

        switch (tipoCursor) {
            case "id":
                filtro = String.format("AND (:cursorId::BIGINT IS NULL OR id %s :cursorId::BIGINT)", operador);
                break;

            case "fecha":
                filtro = String.format("AND (:cursorFecha::TIMESTAMP IS NULL OR fecha_alta %s :cursorFecha::TIMESTAMP)", operador);
                break;

            case "fecha_id":
                if (esHaciaAtras) {
                    filtro = "AND (:cursorFecha::TIMESTAMP IS NULL OR :cursorId::BIGINT IS NULL OR " +
                            "(fecha_alta < :cursorFecha::TIMESTAMP OR " +
                            "(fecha_alta = :cursorFecha::TIMESTAMP AND id < :cursorId::BIGINT)))";
                } else {
                    filtro = "AND (:cursorFecha::TIMESTAMP IS NULL OR :cursorId::BIGINT IS NULL OR " +
                            "(fecha_alta > :cursorFecha::TIMESTAMP OR " +
                            "(fecha_alta = :cursorFecha::TIMESTAMP AND id > :cursorId::BIGINT)))";
                }
                break;

            default:
                filtro = String.format("AND (:cursorId::BIGINT IS NULL OR id %s :cursorId::BIGINT)", operador);
                break;
        }

        return insertarFiltroEnWhere(query, filtro);
    }

    /**
     * Reemplaza LIMIT/OFFSET con límite de cursor
     */
    private String aplicarLimiteCursor(String query) {
        // Remover OFFSET si existe (incompatible con cursor)
        query = query.replaceAll("(?i)\\s+OFFSET\\s+[^\\s]+", "");

        // Reemplazar o agregar LIMIT
        if (query.toUpperCase().contains("LIMIT")) {
            return query.replaceAll("(?i)\\s+LIMIT\\s+[^\\s]+", " LIMIT :limiteCursor");
        } else {
            return query + " LIMIT :limiteCursor";
        }
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private boolean contieneOrdenPor(String query, String campo) {
        String pattern = "(?i)order\\s+by.*\\b" + campo + "\\b";
        return query.matches(".*" + pattern + ".*");
    }

    private String insertarFiltroEnWhere(String query, String filtro) {
        // Buscar WHERE existente
        if (query.toUpperCase().contains("WHERE")) {
            // Insertar antes de GROUP BY, ORDER BY, o LIMIT
            String[] terminadores = {"GROUP BY", "ORDER BY", "LIMIT"};

            for (String terminador : terminadores) {
                if (query.toUpperCase().contains(terminador)) {
                    return query.replaceFirst("(?i)\\s+" + terminador, " " + filtro + " " + terminador);
                }
            }

            // Si no hay terminadores, agregar al final
            return query + " " + filtro;
        } else {
            // Agregar WHERE inicial
            return query.replaceFirst("(?i)\\s+(GROUP BY|ORDER BY|LIMIT)", " WHERE 1=1 " + filtro + " $1");
        }
    }

    private LocalDateTime parsearFecha(String fecha) {
        if (fecha == null) return null;

        try {
            // Intentar diferentes formatos
            if (fecha.contains("T")) {
                return LocalDateTime.parse(fecha, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } else {
                return LocalDateTime.parse(fecha + "T00:00:00");
            }
        } catch (Exception e) {
            log.warn("Error parseando fecha de cursor: {}", fecha);
            return null;
        }
    }

    // =============== MAPEO DE PARÁMETROS EXISTENTES (SIN CAMBIOS) ===============

    private void mapearParametroFechas(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("fechaInicio", filtros.getFechaInicio(), Types.DATE);
        params.addValue("fechaFin", filtros.getFechaFin(), Types.DATE);
        params.addValue("fechaEspecifica", filtros.getFechaEspecifica(), Types.DATE);
    }

    private void mapearParametrosUbicacion(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("provincias", convertirListaAArray(filtros.getProvincias()), Types.ARRAY);
        params.addValue("municipios", convertirListaAArray(filtros.getMunicipios()), Types.ARRAY);
        params.addValue("lugares", convertirListaAArray(filtros.getLugares()), Types.ARRAY);
        params.addValue("partido", convertirListaAArray(filtros.getPartido()), Types.ARRAY);
        params.addValue("concesiones", convertirListaEnterosAArray(filtros.getConcesiones()), Types.ARRAY);
    }

    private void mapearParametrosEquipos(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposDispositivos", convertirListaEnterosAArray(filtros.getTiposDispositivos()), Types.ARRAY);
        params.addValue("patronesEquipos", convertirListaAArray(filtros.getPatronesEquipos()), Types.ARRAY);
        params.addValue("tipoEquipo", convertirListaAArray(filtros.getPatronesEquipos()), Types.ARRAY);
        params.addValue("seriesEquiposExactas", convertirListaAArray(filtros.getSeriesEquiposExactas()), Types.ARRAY);
        params.addValue("filtrarPorTipoEquipo", filtros.getFiltrarPorTipoEquipo(), Types.BOOLEAN);
        params.addValue("incluirSE", filtros.getIncluirSE(), Types.BOOLEAN);
        params.addValue("incluirVLR", filtros.getIncluirVLR(), Types.BOOLEAN);
    }

    private void mapearParametrosInfracciones(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposInfracciones", convertirListaEnterosAArray(filtros.getTiposInfracciones()), Types.ARRAY);
        params.addValue("estadosInfracciones", convertirListaEnterosAArray(filtros.getEstadosInfracciones()), Types.ARRAY);
        params.addValue("exportadoSacit", filtros.getExportadoSacit(), Types.BOOLEAN);
    }

    private void mapearParametrosDominios(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("tiposVehiculos", convertirListaAArray(filtros.getTipoVehiculo()), Types.ARRAY);
        params.addValue("tieneEmail", filtros.getTieneEmail(), Types.BOOLEAN);
        params.addValue("tipoDocumento", filtros.getTipoDocumento(), Types.VARCHAR);
    }

    private void mapearParametrosAdicionales(ParametrosFiltrosDTO filtros, MapSqlParameterSource params) {
        params.addValue("provincia", null, Types.VARCHAR);
        params.addValue("fechaReporte", null, Types.DATE);
    }

    private String[] convertirListaAArray(List<String> lista) {
        return lista != null && !lista.isEmpty() ? lista.toArray(new String[0]) : null;
    }

    private Integer[] convertirListaEnterosAArray(List<Integer> lista) {
        return lista != null && !lista.isEmpty() ? lista.toArray(new Integer[0]) : null;
    }

    public void logParametros(MapSqlParameterSource params) {
        if (log.isDebugEnabled()) {
            for (String paramName : params.getParameterNames()) {
                Object value = params.getValue(paramName);
                log.debug("Parámetro: {} = {} ({})",
                        paramName, value, value != null ? value.getClass().getSimpleName() : "null");
            }
        }
    }
}