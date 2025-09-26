package org.transito_seguro.utils;

import lombok.extern.slf4j.Slf4j;
import org.transito_seguro.enums.TipoCampo;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.dto.ParametrosFiltrosDTO.esParametroFechaBasico;

@Slf4j
public final class RegexUtils {

    private RegexUtils() {}

    // =============== PATTERNS EXISTENTES ===============

    public static final Pattern PATTERN_WHERE_FIELD = Pattern.compile(
            "\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.?([a-zA-Z_][a-zA-Z0-9_]*)\\s*(=|!=|<>|>=|<=|>|<|LIKE|ILIKE|IN)\\s*:([a-zA-Z_][a-zA-Z0-9_]*)",
            Pattern.CASE_INSENSITIVE
    );

    public static final Pattern PATTERN_ID = Pattern.compile("(?i).*(id|codigo|serial|clave).*");
    public static final Pattern PATTERN_FECHA = Pattern.compile("(?i).*(fecha|date|time|timestamp|created|updated).*");
    public static final Pattern PATTERN_ESTADO = Pattern.compile("(?i).*(estado|status|activo|habilitado|eliminado).*");
    public static final Pattern PATTERN_EMAIL = Pattern.compile("(?i).*(email|mail|correo).*");
    public static final Pattern PATTERN_TELEFONO = Pattern.compile("(?i).*(telefono|phone|celular|movil).*");
    public static final Pattern PATTERN_TEXTO = Pattern.compile("(?i).*(nombre|descripcion|titulo|comment|observ).*");
    public static final Pattern PATTERN_PROVINCIA = Pattern.compile("(?i).*(provincia|contexto|region).*");
    public static final Pattern PATTERN_MUNICIPIO = Pattern.compile("(?i).*(municipio|ciudad|localidad|partido).*");

    public static final Pattern SELECT_PATTERN =
            Pattern.compile("SELECT\\s+(.+?)\\s+FROM", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public static final Pattern CAMPO_AS_PATTERN =
            Pattern.compile("(.+?)\\s+AS\\s+[\"']?([^,\"'\\s]+)[\"']?", Pattern.CASE_INSENSITIVE);

    public static final Pattern SUM_PATTERN =
            Pattern.compile("SUM\\s*\\([^)]+\\)", Pattern.CASE_INSENSITIVE);

    public static final Pattern COUNT_PATTERN =
            Pattern.compile("COUNT\\s*\\([^)]+\\)", Pattern.CASE_INSENSITIVE);

    public static final Pattern CASE_WHEN_PATTERN =
            Pattern.compile("CASE\\s+WHEN.+?END", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // ✅ NUEVO: Pattern para extraer campos tabla.campo del SELECT
    public static final Pattern PATTERN_CAMPO_COMPLETO = Pattern.compile(
            "\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b"
    );

    // =============== MÉTODOS NUEVOS ÚTILES ===============

    /**
     * ✅ NUEVO: Extrae todos los campos tabla.campo del SELECT
     * Retorna un Map con: campo_simple -> tabla.campo
     */
    public static Map<String, String> extraerCamposDelSelect(String sql) {
        Map<String, String> campos = new HashMap<String, String>();

        String selectClause = extraerSelectClause(sql);
        if (selectClause == null) {
            return campos;
        }

        Matcher matcher = PATTERN_CAMPO_COMPLETO.matcher(selectClause);
        while (matcher.find()) {
            String tabla = matcher.group(1);
            String campo = matcher.group(2);
            String nombreCompleto = tabla + "." + campo;
            String campoSimple = campo.toLowerCase();

            campos.put(campoSimple, nombreCompleto);
            log.trace("Campo extraído: {} -> {}", campoSimple, nombreCompleto);
        }

        return campos;
    }

    /**
     * ✅ NUEVO: Clasifica campos por tipo usando patterns
     * Retorna Map con: tipo -> Set<campos>
     */
    public static Map<TipoCampo, Set<String>> clasificarCamposPorTipo(Map<String, String> campos) {
        Map<TipoCampo, Set<String>> clasificacion = new HashMap<TipoCampo, Set<String>>();

        // Inicializar sets
        clasificacion.put(TipoCampo.TIEMPO, new HashSet<String>());
        clasificacion.put(TipoCampo.UBICACION, new HashSet<String>());
        clasificacion.put(TipoCampo.CATEGORIZACION, new HashSet<String>());
        clasificacion.put(TipoCampo.CALCULADO, new HashSet<String>());
        clasificacion.put(TipoCampo.IDENTIFICADOR, new HashSet<String>());

        for (Map.Entry<String, String> entry : campos.entrySet()) {
            String campoSimple = entry.getKey();
            String nombreCompleto = entry.getValue();

            TipoCampo tipo = detectarTipoCampoPorPatterns(campoSimple);
            clasificacion.get(tipo).add(campoSimple);

            log.trace("Campo '{}' clasificado como: {}", campoSimple, tipo);
        }

        return clasificacion;
    }

    /**
     * ✅ NUEVO: Detecta tipo de campo usando patterns de RegexUtils
     */
    public static TipoCampo detectarTipoCampoPorPatterns(String nombreCampo) {
        if (PATTERN_FECHA.matcher(nombreCampo).find()) {
            return TipoCampo.TIEMPO;
        }

        if (PATTERN_PROVINCIA.matcher(nombreCampo).find() ||
                PATTERN_MUNICIPIO.matcher(nombreCampo).find()) {
            return TipoCampo.UBICACION;
        }

        if (PATTERN_TEXTO.matcher(nombreCampo).find()) {
            return TipoCampo.CATEGORIZACION;
        }

        if (PATTERN_ESTADO.matcher(nombreCampo).find()) {
            return TipoCampo.CALCULADO;
        }

        if (PATTERN_ID.matcher(nombreCampo).find()) {
            return TipoCampo.IDENTIFICADOR;
        }

        return TipoCampo.DETALLE; // Default
    }

    /**
     * ✅ NUEVO: Encuentra el mejor campo de un tipo específico
     */
    public static String encontrarMejorCampo(Set<String> campos, String[] prioridades) {
        for (String prioridad : prioridades) {
            for (String campo : campos) {
                if (campo.contains(prioridad)) {
                    return campo;
                }
            }
        }

        // Si no hay match con prioridades, retornar el primero
        return campos.isEmpty() ? null : campos.iterator().next();
    }

    /**
     * ✅ NUEVO: Verifica si el SQL tiene WHERE
     */
    public static boolean tieneWhere(String sql) {
        return sql != null && sql.toUpperCase().contains("WHERE");
    }

    // =============== MÉTODOS EXISTENTES ===============

    public static boolean esExpresionAgregacion(String expresion) {
        return SUM_PATTERN.matcher(expresion).find() || COUNT_PATTERN.matcher(expresion).find() ||
                expresion.toUpperCase().contains("AVG(") ||
                expresion.toUpperCase().contains("MAX(") ||
                expresion.toUpperCase().contains("MIN(");
    }

    public static boolean esExpresionCalculada(String expresion) {
        return CASE_WHEN_PATTERN.matcher(expresion).find() ||
                expresion.contains("COALESCE(") ||
                expresion.contains("TO_CHAR(") ||
                expresion.contains("CONCAT(");
    }

    public static Set<String> extraerFiltros(String sql, Pattern patron) {
        Set<String> filtros = new HashSet<String>();
        Matcher matcher = patron.matcher(sql);
        while (matcher.find()) {
            filtros.add(matcher.group(0));
        }
        return filtros;
    }

    public static String extraerSelectClause(String query) {
        Matcher matcher = RegexUtils.SELECT_PATTERN.matcher(query.trim());
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    public static Set<String> identificarFiltrosFechaExistentes(String sql) {
        Set<String> filtros = new HashSet<String>();
        Pattern patronFecha = Pattern.compile(
                "\\b([a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(=|>=|<=|>|<)\\s*:([a-zA-Z_]*fecha[a-zA-Z_]*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patronFecha.matcher(sql);
        while (matcher.find()) {
            String condicionCompleta = matcher.group(0);
            String parametro = matcher.group(3);

            if (esParametroFechaBasico(parametro)) {
                filtros.add(condicionCompleta);
                log.trace("Filtro de fecha identificado para reemplazo: {}", condicionCompleta);
            }
        }

        return filtros;
    }

    public static Set<String> identificarFiltrosUbicacionExistentes(String sql) {
        Set<String> filtros = new HashSet<String>();
        Pattern patronUbicacion = Pattern.compile(
                "\\b([a-zA-Z_]*(?:provincia|municipio|contexto)[a-zA-Z_]*)\\s*(=|LIKE|ILIKE)\\s*:([a-zA-Z_]+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patronUbicacion.matcher(sql);
        while (matcher.find()) {
            filtros.add(matcher.group(0));
            log.trace("Filtro de ubicación identificado para reemplazo: {}", matcher.group(0));
        }

        return filtros;
    }

    public static Set<String> identificarFiltrosEstadoExistentes(String sql) {
        Set<String> filtros = new HashSet<String>();
        Pattern patronEstado = Pattern.compile(
                "\\b([a-zA-Z_]*(?:exporta|estado|activo|eliminado)[a-zA-Z_]*)\\s*=\\s*:([a-zA-Z_]+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patronEstado.matcher(sql);
        while (matcher.find()) {
            filtros.add(matcher.group(0));
            log.trace("Filtro de estado identificado para reemplazo: {}", matcher.group(0));
        }

        return filtros;
    }

    public static String removerCondicionWhereEspecifica(String sql, String condicionARemover) {
        String condicionEscapada = Pattern.quote(condicionARemover);
        String[] patrones = {
                "\\s*AND\\s*" + condicionEscapada + "\\s*",
                "\\s*OR\\s*" + condicionEscapada + "\\s*",
                condicionEscapada + "\\s*AND\\s*",
                condicionEscapada + "\\s*OR\\s*",
                condicionEscapada
        };

        String resultado = sql;
        for (String patron : patrones) {
            resultado = resultado.replaceAll(patron, " ");
        }

        return limpiarWhereVacio(resultado);
    }

    public static String limpiarWhereVacio(String sql) {
        sql = sql.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");
        sql = sql.replaceAll("(?i)WHERE\\s*$", "");
        sql = sql.replaceAll("(?i)WHERE\\s+(ORDER BY|GROUP BY|LIMIT|OFFSET)", "$1");
        return sql.trim();
    }

    public static boolean detectarWhereEnSqlLimpio(String sql) {
        return sql.toUpperCase().contains("WHERE");
    }

    public static int contarCondicionesEnWhereClause(String whereClause) {
        if (whereClause == null || whereClause.trim().isEmpty()) return 0;
        int andCount = whereClause.split("\\bAND\\b", -1).length - 1;
        int orCount = whereClause.split("\\bOR\\b", -1).length - 1;
        return Math.max(1, andCount + orCount + 1);
    }
}