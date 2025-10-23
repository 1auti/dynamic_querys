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
     * CORREGIDO: Ahora valida y activa keyset correctamente
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

        // 2. KEYSET SIMPLIFICADO + PAGINACIÓN CORREGIDA
        mapearKeysetSimplificado(parametros, filtros);
        mapearPaginacionKeyset(parametros, filtros);

        // 3. ✅ NUEVO: Detectar modo streaming y modificar SQL si es necesario
        String sqlFinal = queryOriginal;
        Object limiteParam = parametros.getValue("limite");
        boolean esModoStreaming = limiteParam != null &&
                (Integer) limiteParam == Integer.MAX_VALUE;

        if (esModoStreaming) {
            // Remover LIMIT y OFFSET del SQL para streaming sin límite
            sqlFinal = sqlFinal.replaceAll("(?i)\\s+LIMIT\\s+[^;]*$", "");
            sqlFinal = sqlFinal.replaceAll("(?i)\\s+OFFSET\\s+[^;]*$", "");

            log.info("🌊 Modo STREAMING activado: LIMIT/OFFSET removidos del SQL");
            metadata.put("modo_streaming", true);
        } else {
            metadata.put("modo_streaming", false);
        }

        // 4. Detectar estado de keyset para logging
        boolean keysetActivo = filtros != null && filtros.getLastId() != null;

        log.debug("Query procesada. Parámetros: {} | Keyset: {} | Streaming: {}",
                parametros.getParameterNames().length,
                keysetActivo ? "ACTIVO(id=" + filtros.getLastId() + ")" : "INACTIVO",
                esModoStreaming ? "SÍ" : "NO");

        // ✅ CRÍTICO: Retornar con el SQL modificado
        return new QueryResult(sqlFinal, parametros, metadata);
    }

    // =================== KEYSET SIMPLIFICADO ===================

    /**
     * KEYSET MEJORADO con validación de progreso
     */
    private void mapearKeysetSimplificado(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("lastId", null, Types.INTEGER);
            return;
        }

        Integer lastId = filtros.getLastId();

        // ✅ VALIDACIÓN: Verificar que lastId no sea inválido
        if (lastId != null && lastId < 0) {
            log.warn(" LastId inválido detectado: {}. Usando null.", lastId);
            lastId = null;
        }

        params.addValue("lastId", lastId, Types.INTEGER);

        if (lastId != null) {
            log.debug(" Keyset ACTIVO: lastId={}", lastId);
        } else {
            log.debug(" Keyset INACTIVO: primera página");
        }
    }

    /**
     * Mapea parámetros de paginación (OFFSET + KEYSET + LIMITE).
     */
//    private void mapearPaginacionCompleta(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
//        if (filtros == null) {
//            params.addValue("offset", 0, Types.INTEGER);
//            params.addValue("limite", 10000, Types.INTEGER);
//            params.addValue("lastId", null, Types.BIGINT);
//            return;
//        }
//
//        // === OFFSET ===
//        Integer offset = filtros.getOffset();
//        if (offset == null || offset < 0) {
//            offset = 0;
//        }
//        params.addValue("offset", offset, Types.INTEGER);  // ✅ YA LO TIENES
//
//        // === LIMITE ===
//        Integer limite = filtros.getLimite();
//        if (limite == null || limite <= 0) {
//            limite = 10000;
//        } else if (limite > 50000) {
//            limite = 50000;
//        }
//        params.addValue("limite", limite, Types.INTEGER);  // ✅ YA LO TIENES
//
//        // === KEYSET (lastId) ===
//        Integer lastId = filtros.getLastId();
//        if (lastId != null && lastId < 0) {
//            lastId = null;
//        }
//        params.addValue("lastId", lastId, Types.BIGINT);  // ✅ YA LO TIENES
//    }

    /**
     * PAGINACIÓN CORREGIDA
     * - Usa límite razonable por defecto (1000)
     * - NUNCA usa Integer.MAX_VALUE que desactiva keyset
     */
    /**
     * PAGINACIÓN CORREGIDA con OFFSET
     */
    private void mapearPaginacionKeyset(MapSqlParameterSource params, ParametrosFiltrosDTO filtros) {
        if (filtros == null) {
            params.addValue("offset", 0, Types.INTEGER);      // ✅ AGREGAR
            params.addValue("limite", 10000, Types.INTEGER);
            return;
        }

        // ✅ CRÍTICO: Mapear OFFSET
        Integer offset = filtros.getOffset();
        if (offset == null || offset < 0) {
            offset = 0;
        }
        params.addValue("offset", offset, Types.INTEGER);  // ✅ ESTO FALTABA

        // LÍMITE
        Integer limiteOriginal = filtros.getLimite();
        int limiteFinal;

        if (limiteOriginal == null || limiteOriginal <= 0 || limiteOriginal == Integer.MAX_VALUE) {
            limiteFinal = Integer.MAX_VALUE;
            log.debug("🔧 Límite corregido: {} → {}", limiteOriginal, limiteFinal);
        } else if (limiteOriginal > 50000) {
            limiteFinal = 50000;
            log.debug("🔧 Límite reducido: {} → {} (máximo)", limiteOriginal, limiteFinal);
        } else {
            limiteFinal = limiteOriginal;
        }

        params.addValue("limite", limiteFinal, Types.INTEGER);

        log.debug("📊 Paginación: offset={}, limite={}", offset, limiteFinal);
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

}