package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.exception.ValidationException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class DynamicBuilderQuery {

    private Map<String, String> subconsultasProtegidas;

    public enum TipoFiltroDetectado {
        FECHA, ESTADO, TIPO_INFRACCION, EXPORTA_SACIT, CONCESION
    }

    @Getter
    @AllArgsConstructor
    public static class AnalisisFiltros {
        private final Set<TipoFiltroDetectado> filtrosDetectados;
        private final Map<TipoFiltroDetectado, String> camposPorFiltro;

        public boolean tiene(TipoFiltroDetectado tipo) {
            return filtrosDetectados.contains(tipo);
        }

        public String getCampo(TipoFiltroDetectado tipo) {
            return camposPorFiltro.get(tipo);
        }
    }

    // =============== MÉTODO PRINCIPAL ===============

    public String construirSqlInteligente(String sqlOriginal) {
        log.debug("Iniciando construcción inteligente de SQL con Keyset");

        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        // 1. Limpiar y proteger
        String sql = limpiarComentariosSQL(sqlOriginal);
        sql = removerTerminadoresTemporalmente(sql);
        sql = protegerSubconsultas(sql);

        // 2. Preparar para Keyset
        sql = prepararQueryParaKeyset(sql);

        // 3. Analizar y procesar filtros
        AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sql);
        sql = removerFiltrosHardcodeados(sql, analisisFiltros);
        sql = restaurarSubconsultas(sql);
        sql = agregarFiltrosDinamicos(sql, analisisFiltros);

        // 4. Agregar Keyset + paginación
        sql = agregarKeysetPagination(sql);

        log.debug("SQL construido: {} caracteres", sql.length());
        return sql;
    }

    // =============== PREPARACIÓN KEYSET ===============

    private String prepararQueryParaKeyset(String sql) {
        // NUEVO: Detectar si es una query de consolidación
        if (esQueryDeConsolidacion(sql)) {
            log.debug("Query de consolidación detectada - omitiendo lógica de Keyset");
            return sql; // No agregar i.id ni lógica de Keyset
        }

        // Lógica original para queries normales
        if (!tieneIdEnSelect(sql)) {
            sql = agregarIdAlSelect(sql);
        }

        if (usaGroupByNumerico(sql)) {
            sql = ajustarGroupByConId(sql);
        }

        return sql;
    }

    // NUEVO MÉTODO
    private boolean esQueryDeConsolidacion(String sql) {
        String upper = sql.toUpperCase();

        // Detectar si tiene GROUP BY y funciones de agregación
        boolean tieneGroupBy = upper.contains("GROUP BY");
        boolean tieneFuncionAgregada = Pattern.compile(
                "\\b(COUNT|SUM|AVG|MIN|MAX)\\s*\\(",
                Pattern.CASE_INSENSITIVE
        ).matcher(sql).find();

        return tieneGroupBy && tieneFuncionAgregada;
    }

    private boolean tieneIdEnSelect(String sql) {
        return Pattern.compile("SELECT\\s+.*\\bi\\.id\\b.*FROM",
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL)
                .matcher(sql).find();
    }

    private String agregarIdAlSelect(String sql) {
        Pattern pattern = Pattern.compile(
                "(SELECT\\s+)(DISTINCT\\s+)?(.*?)(\\s+FROM)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String select = matcher.group(1);
            String distinct = matcher.group(2);
            String campos = matcher.group(3).trim();
            String from = matcher.group(4);

            String nuevoSelect = distinct != null
                    ? select + distinct + "i.id, " + campos + from
                    : select + "i.id, " + campos + from;

            log.debug("i.id agregado (DISTINCT: {})", distinct != null);
            return matcher.replaceFirst(Matcher.quoteReplacement(nuevoSelect));
        }

        return sql;
    }

    private boolean usaGroupByNumerico(String sql) {
        return Pattern.compile("GROUP\\s+BY\\s+\\d+", Pattern.CASE_INSENSITIVE)
                .matcher(sql).find();
    }

    private String ajustarGroupByConId(String sql) {
        Pattern pattern = Pattern.compile(
                "(GROUP\\s+BY\\s+)(\\d+(?:\\s*,\\s*\\d+)*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String groupByClause = matcher.group(1);
            String numeros = matcher.group(2);
            String[] nums = numeros.split("\\s*,\\s*");

            // i.id es el primer campo, todos los demás se desplazan +1
            StringBuilder nuevo = new StringBuilder("1");
            for (String num : nums) {
                int nuevoNum = Integer.parseInt(num.trim()) + 1;
                nuevo.append(", ").append(nuevoNum);
            }

            log.debug("GROUP BY ajustado: {} → {}", numeros, nuevo);
            return matcher.replaceFirst(Matcher.quoteReplacement(groupByClause + nuevo));
        }

        return sql;
    }

    // =============== KEYSET PAGINATION ===============

    private String agregarKeysetPagination(String sql) {

        if (esQueryDeConsolidacion(sql)) {
            // Solo agregar LIMIT si no existe
            if (!sql.toUpperCase().contains("LIMIT")) {
                sql += "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
            }
            return sql;
        }

        sql = agregarCondicionKeyset(sql);

        if (!sql.toUpperCase().contains("ORDER BY")) {
            sql = agregarOrderByKeyset(sql);
        }

        if (!sql.toUpperCase().contains("LIMIT")) {
            sql += "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
        }

        return sql;
    }

    private String agregarCondicionKeyset(String sql) {
        // Detectar si la query usa DISTINCT
        boolean tieneDistinct = Pattern.compile(
                "SELECT\\s+DISTINCT",
                Pattern.CASE_INSENSITIVE
        ).matcher(sql).find();

        String keyset;
        if (tieneDistinct) {
            // Versión SIN COALESCE para DISTINCT (debe coincidir exactamente con SELECT)
            keyset = "  AND (:lastId::INTEGER IS NULL OR \n" +
                    "       (i.id > :lastId::INTEGER OR \n" +
                    "        (i.id = :lastId::INTEGER AND pc.serie_equipo > :lastSerieEquipo::TEXT) OR \n" +
                    "        (i.id = :lastId::INTEGER AND pc.serie_equipo = :lastSerieEquipo::TEXT \n" +
                    "         AND pc.lugar > :lastLugar::TEXT)))\n";
        } else {
            // Versión con COALESCE para queries normales (maneja NULLs correctamente)
            keyset = "  AND (:lastId::INTEGER IS NULL OR \n" +
                    "       (i.id > :lastId::INTEGER OR \n" +
                    "        (i.id = :lastId::INTEGER AND COALESCE(pc.serie_equipo, '') > COALESCE(:lastSerieEquipo::TEXT, '')) OR \n" +
                    "        (i.id = :lastId::INTEGER AND COALESCE(pc.serie_equipo, '') = COALESCE(:lastSerieEquipo::TEXT, '') \n" +
                    "         AND COALESCE(pc.lugar, '') > COALESCE(:lastLugar::TEXT, ''))))\n";
        }

        String upper = sql.toUpperCase();

        // Insertar ANTES de GROUP BY
        if (upper.contains("GROUP BY")) {
            return sql.replaceFirst("(?i)\\s+GROUP\\s+BY", "\n" + keyset + "GROUP BY");
        }
        // Si no hay GROUP BY, insertar antes de ORDER BY
        else if (upper.contains("ORDER BY")) {
            return sql.replaceFirst("(?i)\\s+ORDER\\s+BY", "\n" + keyset + "ORDER BY");
        }
        // Si no hay nada, agregar al final
        else {
            return sql + "\n" + keyset;
        }
    }

    private String agregarOrderByKeyset(String sql) {
        // Verificar si tiene DISTINCT
        boolean tieneDistinct = Pattern.compile(
                "SELECT\\s+DISTINCT",
                Pattern.CASE_INSENSITIVE
        ).matcher(sql).find();

        String orderBy;
        if (tieneDistinct) {
            // Para DISTINCT, ORDER BY debe usar las columnas exactas del SELECT
            orderBy = "\nORDER BY i.id ASC, pc.serie_equipo ASC, pc.lugar ASC";
        } else {
            // Sin DISTINCT, podemos usar COALESCE
            orderBy = "\nORDER BY i.id ASC, " +
                    "COALESCE(pc.serie_equipo, '') ASC, " +
                    "COALESCE(pc.lugar, '') ASC";
        }

        String upper = sql.toUpperCase();

        if (upper.contains("LIMIT")) {
            return sql.replaceFirst("(?i)\\s+LIMIT", orderBy + "\nLIMIT");
        } else if (upper.contains("GROUP BY")) {
            Pattern p = Pattern.compile(
                    "(.*GROUP\\s+BY\\s+[^;]+?)(\\s*$|\\s*;)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
            Matcher m = p.matcher(sql);
            if (m.find()) {
                return m.group(1) + orderBy + m.group(2);
            }
        }

        return sql + orderBy;
    }

    // =============== PROTECCIÓN SUBCONSULTAS ===============

    private String protegerSubconsultas(String sql) {
        subconsultasProtegidas = new HashMap<>();
        sql = protegerPatron(sql, "EXISTS\\s*\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)", "EXISTS");
        sql = protegerPatron(sql, "\\(\\s*SELECT[^()]*(?:\\([^()]*\\)[^()]*)*\\)", "SUBSELECT");
        log.debug("Protegidas {} subconsultas", subconsultasProtegidas.size());
        return sql;
    }

    private String protegerPatron(String sql, String patronStr, String prefijo) {
        Pattern patron = Pattern.compile(patronStr, Pattern.CASE_INSENSITIVE);
        Matcher matcher = patron.matcher(sql);
        StringBuffer sb = new StringBuffer();
        int contador = 0;

        while (matcher.find()) {
            String placeholder = "___" + prefijo + "_" + contador + "___";
            subconsultasProtegidas.put(placeholder, matcher.group(0));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(placeholder));
            contador++;
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String restaurarSubconsultas(String sql) {
        if (subconsultasProtegidas != null) {
            for (Map.Entry<String, String> entry : subconsultasProtegidas.entrySet()) {
                sql = sql.replace(entry.getKey(), entry.getValue());
            }
            subconsultasProtegidas = null;
        }
        return sql;
    }

    // =============== LIMPIEZA ===============

    private String limpiarComentariosSQL(String sql) {
        return sql.replaceAll("--[^\n\r]*", "")
                .replaceAll("/\\*.*?\\*/", "")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String removerTerminadoresTemporalmente(String sql) {
        return sql.replaceAll(";\\s*$", "").trim();
    }

    // =============== ANÁLISIS FILTROS ===============

    private AnalisisFiltros analizarFiltrosExistentes(String sql) {
        Set<TipoFiltroDetectado> filtros = new HashSet<>();
        Map<TipoFiltroDetectado, String> campos = new HashMap<>();

        Pattern wherePattern = Pattern.compile(
                "WHERE\\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|;|$)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher whereMatcher = wherePattern.matcher(sql);

        if (whereMatcher.find()) {
            String whereClause = whereMatcher.group(1).trim();
            detectarFiltros(whereClause, filtros, campos);
        }

        return new AnalisisFiltros(filtros, campos);
    }

    private void detectarFiltros(String where, Set<TipoFiltroDetectado> filtros,
                                 Map<TipoFiltroDetectado, String> campos) {
        // Fecha
        Matcher m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=)",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.FECHA);
            campos.put(TipoFiltroDetectado.FECHA, m.group(1));
        }

        // Estado
        m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_estado)\\s+IN",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.ESTADO);
            campos.put(TipoFiltroDetectado.ESTADO, m.group(1));
        }

        // Tipo infracción
        m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_tipo_infra)\\s+IN",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.TIPO_INFRACCION);
            campos.put(TipoFiltroDetectado.TIPO_INFRACCION, m.group(1));
        }

        // Exporta SACIT
        m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.exporta_sacit)\\s*=",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.EXPORTA_SACIT);
            campos.put(TipoFiltroDetectado.EXPORTA_SACIT, m.group(1));
        }
    }

    // =============== REMOVER FILTROS ===============

    private String removerFiltrosHardcodeados(String sql, AnalisisFiltros analisis) {
        for (TipoFiltroDetectado tipo : analisis.getFiltrosDetectados()) {
            sql = removerFiltro(sql, tipo, analisis.getCampo(tipo));
        }
        return limpiarWhereVacio(sql);
    }

    private String removerFiltro(String sql, TipoFiltroDetectado tipo, String campo) {
        String escapado = Pattern.quote(campo);

        switch (tipo) {
            case FECHA:
                sql = sql.replaceAll(escapado + "\\s*(>=|>|<|<=|=)\\s*(DATE\\s*)?'[^']*'", "");
                sql = sql.replaceAll(escapado + "\\s*(>=|>)\\s*TO_DATE\\([^)]+\\)", "");
                break;
            case ESTADO:
            case TIPO_INFRACCION:
                sql = sql.replaceAll(escapado + "\\s+IN\\s*\\([^)]+\\)", "");
                break;
            case EXPORTA_SACIT:
                sql = sql.replaceAll(escapado + "\\s*=\\s*(true|false)", "");
                break;
        }

        return sql;
    }

    private String limpiarWhereVacio(String sql) {
        for (int i = 0; i < 3; i++) {
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(AND|OR)\\s+", " $2 ");
            sql = sql.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(GROUP BY|ORDER BY|LIMIT|$)", " $2");
        }
        return sql.replaceAll("(?i)WHERE\\s+(GROUP BY|ORDER BY|LIMIT|$)", "$1")
                .replaceAll("\\s+", " ").trim();
    }

    // =============== AGREGAR FILTROS DINÁMICOS ===============

    private String agregarFiltrosDinamicos(String sql, AnalisisFiltros analisis) {
        boolean tieneWhere = detectarWhere(sql);
        StringBuilder filtros = new StringBuilder();

        if (analisis.tiene(TipoFiltroDetectado.FECHA)) {
            agregarFiltroFecha(filtros, tieneWhere, analisis.getCampo(TipoFiltroDetectado.FECHA));
            tieneWhere = true;
        }

        if (analisis.tiene(TipoFiltroDetectado.ESTADO)) {
            agregarFiltroEstado(filtros, tieneWhere, analisis.getCampo(TipoFiltroDetectado.ESTADO));
            tieneWhere = true;
        }

        if (analisis.tiene(TipoFiltroDetectado.TIPO_INFRACCION)) {
            agregarFiltroTipoInfraccion(filtros, tieneWhere,
                    analisis.getCampo(TipoFiltroDetectado.TIPO_INFRACCION));
            tieneWhere = true;
        }

        if (analisis.tiene(TipoFiltroDetectado.EXPORTA_SACIT)) {
            agregarFiltroExportaSacit(filtros, tieneWhere,
                    analisis.getCampo(TipoFiltroDetectado.EXPORTA_SACIT));
        }

        if (filtros.length() > 0) {
            return insertarFiltros(sql, filtros.toString());
        }

        return sql;
    }

    private boolean detectarWhere(String sql) {
        String upper = sql.toUpperCase();
        int nivel = 0;

        for (int i = 0; i < sql.length() - 5; i++) {
            if (sql.charAt(i) == '(') nivel++;
            else if (sql.charAt(i) == ')') nivel--;

            if (nivel == 0 && upper.substring(i, Math.min(i + 6, upper.length())).equals("WHERE ")) {
                return true;
            }
        }
        return false;
    }

    private String insertarFiltros(String sql, String filtros) {
        Pattern p = Pattern.compile(
                "(.*?)(\\s+(GROUP BY|ORDER BY|LIMIT).*)$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher m = p.matcher(sql);
        if (m.matches()) {
            return m.group(1).trim() + "\n" + filtros + "\n" + m.group(2);
        }

        return sql + "\n" + filtros;
    }

    private void agregarFiltroFecha(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND (" : "\nWHERE (");
        sb.append("\n    (:fechaEspecifica::DATE IS NULL OR DATE(").append(campo)
                .append(") = :fechaEspecifica::DATE)")
                .append("\n    AND (:fechaInicio::DATE IS NULL OR DATE(").append(campo)
                .append(") >= :fechaInicio::DATE)")
                .append("\n    AND (:fechaFin::DATE IS NULL OR DATE(").append(campo)
                .append(") <= :fechaFin::DATE)\n  )");
    }

    private void agregarFiltroEstado(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");
    }

    private void agregarFiltroTipoInfraccion(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:tiposInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:tiposInfracciones::INTEGER[]))");
    }

    private void agregarFiltroExportaSacit(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");
    }
}