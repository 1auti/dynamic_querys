package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoFiltroDetectado;
import org.transito_seguro.exception.ValidationException;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class DynamicBuilderQuery {

    private final PaginationStrategyAnalyzer paginationStrategyAnalyzer;
    private Map<String, String> subconsultasProtegidas;

    // ==================== INNER CLASS ====================

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

    // ==================== MÉTODO PRINCIPAL ====================

    /**
     * Construye SQL inteligente con paginación automática
     * Determina y aplica la mejor estrategia de paginación
     */
    public String construirSqlInteligente(String sqlOriginal) {
        log.debug("Iniciando construcción inteligente de SQL");

        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        // 1. Limpiar y proteger
        String sql = limpiarComentariosSQL(sqlOriginal);
        sql = removerTerminadoresTemporalmente(sql);
        sql = protegerSubconsultas(sql);

        // 2. Determinar estrategia de paginación
        AnalisisPaginacion analisisPaginacion = paginationStrategyAnalyzer.determinarEstrategia(sql);
        log.info("Estrategia seleccionada: {} - {}",
                analisisPaginacion.getEstrategiaPaginacion(),
                analisisPaginacion.getRazon());


        // 4. Analizar y procesar filtros
        AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sql);
        sql = removerFiltrosHardcodeados(sql, analisisFiltros);
        sql = restaurarSubconsultas(sql);
        sql = agregarFiltrosDinamicos(sql, analisisFiltros);

        // 5. Aplicar paginación según estrategia
        sql = aplicarPaginacionSegunEstrategia(sql, analisisPaginacion);

        log.debug("SQL construido: {} caracteres", sql.length());
        return sql;
    }

    // ==================== APLICAR PAGINACIÓN SEGÚN ESTRATEGIA ====================

    /**
     * Aplica la paginación según la estrategia determinada
     */
    private String aplicarPaginacionSegunEstrategia(String sql, AnalisisPaginacion analisis) {
        EstrategiaPaginacion estrategia = analisis.getEstrategiaPaginacion();

        switch (estrategia) {
            case KEYSET_CON_ID:
            case KEY_COMPUESTO:
            case KEYSET_CONSOLIDADO:
                //  UNIFICAR TODAS LAS ESTRATEGIAS KEYSET A UNA SOLA
                return aplicarKeysetSimplificado(sql);

        
            case FALLBACK_LIMIT_ONLY:
                return aplicarLimitSimple(sql);

            case SIN_PAGINACION:
                return aplicarLimitParaConsolidacion(sql);

            default:
                log.warn("Estrategia desconocida, aplicando LIMIT simple");
                return aplicarLimitSimple(sql);
        }
    }

    /**
     *  KEYSET SIMPLIFICADO - Solo lastId para todas las queries
     */
    private String aplicarKeysetSimplificado(String sql) {
        log.debug("🔑 Aplicando KEYSET SIMPLIFICADO (solo lastId)");

        // Construir condición keyset simple
        String keysetCondition = "\n  AND (:lastId::INTEGER IS NULL OR id_infraccion > :lastId::INTEGER)";

        // Insertar condición
        sql = insertarCondicionKeyset(sql, keysetCondition);

        // Agregar ORDER BY por id_infraccion
        sql = insertarOrderBy(sql, "\nORDER BY id_infraccion ASC");

        // Agregar LIMIT
        sql = agregarLimitSiNoExiste(sql);

        return sql;
    }

   
    /**
     * Aplica solo LIMIT sin condiciones keyset
     */
    private String aplicarLimitSimple(String sql) {
        log.debug("Aplicando LIMIT simple");
        return agregarLimitSiNoExiste(sql);
    }

    /**
     * Aplica LIMIT para queries consolidadas (GROUP BY)
     */
    private String aplicarLimitParaConsolidacion(String sql) {
        log.debug("Aplicando LIMIT para query consolidada");
        return agregarLimitSiNoExiste(sql);
    }

    /**
     * Agrega LIMIT solo si no existe ya en el SQL
     */
    private String agregarLimitSiNoExiste(String sql) {
        String upper = sql.toUpperCase();

        // Si ya tiene LIMIT, no agregar otro
        if (upper.contains("LIMIT")) {
            log.debug("LIMIT ya existe, no se agrega duplicado");
            return sql;
        }

        return sql + "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
    }

    // ==================== UTILIDADES DE INSERCIÓN ====================

    private String insertarCondicionKeyset(String sql, String condicion) {
        String upper = sql.toUpperCase();

        // Insertar después del WHERE si existe
        if (upper.contains("WHERE")) {
            // Buscar el WHERE y insertar después
            Pattern wherePattern = Pattern.compile(
                    "(WHERE\\s+.*?)(?=(?:GROUP BY|ORDER BY|LIMIT|$))", 
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
            
            Matcher matcher = wherePattern.matcher(sql);
            if (matcher.find()) {
                String whereSection = matcher.group(1);
                String nuevaWhereSection = whereSection + condicion;
                return matcher.replaceFirst(Matcher.quoteReplacement(nuevaWhereSection));
            }
        }
        // Si no hay WHERE, agregar uno
        else {
            // Buscar donde insertar el WHERE (después de FROM/JOIN)
            Pattern fromPattern = Pattern.compile(
                    "(FROM\\s+.*?)(?=(?:WHERE|GROUP BY|ORDER BY|LIMIT|$))", 
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
            
            Matcher matcher = fromPattern.matcher(sql);
            if (matcher.find()) {
                String fromSection = matcher.group(1);
                String nuevaSection = fromSection + "\nWHERE 1=1" + condicion;
                return matcher.replaceFirst(Matcher.quoteReplacement(nuevaSection));
            }
        }

        // Fallback: agregar al final
        return sql + "\nWHERE 1=1" + condicion;
    }

    private String insertarOrderBy(String sql, String orderBy) {
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

    // ==================== PROTECCIÓN SUBCONSULTAS ====================

    private String protegerSubconsultas(String sql) {
        subconsultasProtegidas = new HashMap<>();

        // Proteger expresiones CASE (incluyendo CASE anidados)
        sql = protegerExpresionesCase(sql);

        sql = protegerPatron(sql,
                "EXISTS\\s*\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                "EXISTS");

        sql = protegerPatron(sql,
                "\\(\\s*SELECT[^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                "SUBSELECT");

        log.debug("Protegidas {} subconsultas y expresiones", subconsultasProtegidas.size());
        return sql;
    }

    /**
     * Protege expresiones CASE WHEN ... END respetando anidamiento
     */
    private String protegerExpresionesCase(String sql) {
        Pattern casePattern = Pattern.compile(
                "\\bCASE\\b",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = casePattern.matcher(sql);
        StringBuilder resultado = new StringBuilder();
        int lastEnd = 0;
        int contador = 0;

        while (matcher.find()) {
            resultado.append(sql, lastEnd, matcher.start());

            // Encontrar el END correspondiente
            int inicio = matcher.start();
            int fin = encontrarEndDeCase(sql, matcher.end());

            if (fin > 0) {
                String expresionCase = sql.substring(inicio, fin);
                String placeholder = "___CASE_" + contador + "___";
                subconsultasProtegidas.put(placeholder, expresionCase);
                resultado.append(placeholder);
                lastEnd = fin;
                contador++;
            } else {
                // Si no encuentra END, mantener el CASE original
                resultado.append(sql, inicio, matcher.end());
                lastEnd = matcher.end();
            }
        }

        resultado.append(sql.substring(lastEnd));
        return resultado.toString();
    }

    /**
     * Encuentra la posición del END que cierra un CASE, respetando anidamiento
     */
    private int encontrarEndDeCase(String sql, int inicioDesdeCase) {
        String upper = sql.toUpperCase();
        int nivelCase = 1;
        int pos = inicioDesdeCase;

        while (pos < sql.length() && nivelCase > 0) {
            // Buscar siguiente CASE o END
            int nextCase = upper.indexOf("CASE", pos);
            int nextEnd = upper.indexOf("END", pos);

            // Ajustar para que sean palabras completas
            if (nextCase >= 0 && !esLimiteDePalabra(sql, nextCase, nextCase + 4)) {
                nextCase = -1;
            }
            if (nextEnd >= 0 && !esLimiteDePalabra(sql, nextEnd, nextEnd + 3)) {
                nextEnd = -1;
            }

            if (nextEnd < 0) {
                return -1; // No se encontró END
            }

            if (nextCase >= 0 && nextCase < nextEnd) {
                // Hay un CASE anidado
                nivelCase++;
                pos = nextCase + 4;
            } else {
                // Encontramos un END
                nivelCase--;
                if (nivelCase == 0) {
                    return nextEnd + 3; // Posición después de END
                }
                pos = nextEnd + 3;
            }
        }

        return -1;
    }

    /**
     * Verifica si una posición es límite de palabra (no alfanumérico antes/después)
     */
    private boolean esLimiteDePalabra(String sql, int inicio, int fin) {
        if (inicio > 0 && Character.isLetterOrDigit(sql.charAt(inicio - 1))) {
            return false;
        }
        if (fin < sql.length() && Character.isLetterOrDigit(sql.charAt(fin))) {
            return false;
        }
        return true;
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

    // ==================== LIMPIEZA ====================

    private String limpiarComentariosSQL(String sql) {
        return sql.replaceAll("--[^\n\r]*", "")
                .replaceAll("/\\*.*?\\*/", "")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String removerTerminadoresTemporalmente(String sql) {
        return sql.replaceAll(";\\s*$", "").trim();
    }

    // ==================== ANÁLISIS FILTROS ====================

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
        // Fecha - Detectar comparaciones y BETWEEN
        Matcher m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=|BETWEEN)",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.FECHA);
            campos.put(TipoFiltroDetectado.FECHA, m.group(1));
        }

        // Estado - Detectar IN y NOT IN
        m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_estado)\\s+(?:NOT\\s+)?IN",
                Pattern.CASE_INSENSITIVE
        ).matcher(where);
        if (m.find()) {
            filtros.add(TipoFiltroDetectado.ESTADO);
            campos.put(TipoFiltroDetectado.ESTADO, m.group(1));
        }

        // Tipo infracción - Detectar IN y NOT IN
        m = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_tipo_infra)\\s+(?:NOT\\s+)?IN",
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

    // ==================== REMOVER FILTROS ====================

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
                // 1. Remover BETWEEN (debe ir primero porque incluye AND)
                sql = sql.replaceAll(
                        "(?i)" + escapado + "\\s+BETWEEN\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))\\s+AND\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))",
                        ""
                );

                // 2. Remover comparaciones simples con fechas literales
                sql = sql.replaceAll(
                        "(?i)" + escapado + "\\s*(?:>=|>|<=|<|=)\\s*(?:DATE\\s+)?'[^']*'",
                        ""
                );

                // 3. Remover comparaciones con TO_DATE
                sql = sql.replaceAll(
                        "(?i)" + escapado + "\\s*(?:>=|>|<=|<|=)\\s*TO_DATE\\s*\\([^)]+\\)",
                        ""
                );

                // 4. Remover comparaciones con CAST/CONVERT a DATE
                sql = sql.replaceAll(
                        "(?i)" + escapado + "\\s*(?:>=|>|<=|<|=)\\s*(?:CAST|CONVERT)\\s*\\([^)]+\\)",
                        ""
                );

                break;

            case ESTADO:
            case TIPO_INFRACCION:
                // Remover IN con lista de valores (incluyendo NOT IN)
                sql = sql.replaceAll("(?i)" + escapado + "\\s+(?:NOT\\s+)?IN\\s*\\([^)]+\\)", "");

                // Remover comparación con valor único
                sql = sql.replaceAll("(?i)" + escapado + "\\s*=\\s*\\d+", "");
                break;

            case EXPORTA_SACIT:
                // Remover comparación booleana
                sql = sql.replaceAll("(?i)" + escapado + "\\s*=\\s*(?:true|false|TRUE|FALSE)", "");
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

    // ==================== AGREGAR FILTROS DINÁMICOS ====================

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