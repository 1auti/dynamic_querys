package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoFiltroDetectado;
import org.transito_seguro.exception.ValidationException;
import org.transito_seguro.model.CampoKeyset;
import org.transito_seguro.model.consolidacion.analisis.AnalisisPaginacion;

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

        // 3. Preparar query según estrategia
        sql = prepararQuerySegunEstrategia(sql, analisisPaginacion);

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

    // ==================== PREPARACIÓN SEGÚN ESTRATEGIA ====================

    /**
     * Prepara la query según la estrategia determinada
     */
    private String prepararQuerySegunEstrategia(String sql, AnalisisPaginacion analisis) {
        EstrategiaPaginacion estrategia = analisis.getEstrategiaPaginacion();

        switch (estrategia) {
            case KEYSET_CON_ID:
                // Asegurar que i.id esté en el SELECT
                if (!analisis.isTieneIdInfracciones()) {
                    sql = agregarIdAlSelect(sql);
                }
                // Ajustar GROUP BY si usa numeración
                if (usaGroupByNumerico(sql)) {
                    sql = ajustarGroupByConId(sql, true);
                }
                break;

            case KEY_COMPUESTO:
                // No necesita i.id, usar solo los campos disponibles
                if (usaGroupByNumerico(sql)) {
                    sql = ajustarGroupByConId(sql, false);
                }
                break;

            case SIN_PAGINACION:
            case OFFSET:
            case FALLBACK_LIMIT_ONLY:
                // No requiere preparación especial
                break;
        }

        return sql;
    }

    private boolean tieneIdEnSelect(String sql) {
        return Pattern.compile("SELECT\\s+.*\\b(i\\.id|infracciones\\.id)\\b.*FROM",
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

            log.debug("i.id agregado al SELECT (DISTINCT: {})", distinct != null);
            return matcher.replaceFirst(Matcher.quoteReplacement(nuevoSelect));
        }

        return sql;
    }

    private boolean usaGroupByNumerico(String sql) {
        return Pattern.compile("GROUP\\s+BY\\s+\\d+", Pattern.CASE_INSENSITIVE)
                .matcher(sql).find();
    }

    private String ajustarGroupByConId(String sql, boolean tieneId) {
        Pattern pattern = Pattern.compile(
                "(GROUP\\s+BY\\s+)(\\d+(?:\\s*,\\s*\\d+)*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String groupByClause = matcher.group(1);
            String numeros = matcher.group(2);
            String[] nums = numeros.split("\\s*,\\s*");

            // Extraer campos del SELECT para verificar funciones de agregación
            List<Integer> columnasValidas = filtrarColumnasAgregacion(sql, nums);

            if (columnasValidas.isEmpty()) {
                log.warn("No hay columnas válidas para GROUP BY después del filtrado");
                return sql;
            }

            // Construir nuevo GROUP BY
            StringBuilder nuevo = new StringBuilder();

            if (tieneId) {
                // Si agregamos i.id al inicio, desplazar todas las columnas +1
                nuevo.append("1"); // i.id
                for (Integer numCol : columnasValidas) {
                    nuevo.append(", ").append(numCol + 1);
                }
            } else {
                // Si NO agregamos i.id, usar los números originales filtrados
                nuevo.append(columnasValidas.get(0));
                for (int i = 1; i < columnasValidas.size(); i++) {
                    nuevo.append(", ").append(columnasValidas.get(i));
                }
            }

            log.debug("GROUP BY ajustado: {} → {}", numeros, nuevo);
            return matcher.replaceFirst(Matcher.quoteReplacement(groupByClause + nuevo));
        }

        return sql;
    }

    /**
     * Filtra columnas del GROUP BY que NO son funciones de agregación
     */
    private List<Integer> filtrarColumnasAgregacion(String sql, String[] nums) {
        List<Integer> columnasValidas = new ArrayList<>();

        // Extraer SELECT clause
        Pattern selectPattern = Pattern.compile(
                "SELECT\\s+(DISTINCT\\s+)?(.*?)\\s+FROM",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher selectMatcher = selectPattern.matcher(sql);

        if (!selectMatcher.find()) {
            // Si no podemos extraer SELECT, asumir que todas son válidas
            for (String num : nums) {
                columnasValidas.add(Integer.parseInt(num.trim()));
            }
            return columnasValidas;
        }

        String selectClause = selectMatcher.group(2);

        // Dividir por comas respetando paréntesis
        List<String> campos = dividirCamposSelect(selectClause);

        // Verificar cada número del GROUP BY
        for (String num : nums) {
            int indice = Integer.parseInt(num.trim());

            // Verificar que el índice sea válido (1-based)
            if (indice > 0 && indice <= campos.size()) {
                String campo = campos.get(indice - 1); // Convertir a 0-based

                // Verificar si NO es una función de agregación
                if (!esFuncionAgregacion(campo)) {
                    columnasValidas.add(indice);
                } else {
                    log.debug("Columna {} omitida del GROUP BY (función agregación): {}",
                            indice, campo.substring(0, Math.min(50, campo.length())));
                }
            }
        }

        return columnasValidas;
    }

    /**
     * Verifica si un campo es una función de agregación
     */
    private boolean esFuncionAgregacion(String campo) {
        String upper = campo.trim().toUpperCase();

        // Funciones de agregación comunes
        String[] funciones = {"COUNT(", "SUM(", "AVG(", "MIN(", "MAX(",
                "STRING_AGG(", "ARRAY_AGG(", "JSON_AGG("};

        for (String funcion : funciones) {
            if (upper.startsWith(funcion)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Divide campos del SELECT respetando paréntesis y comillas
     */
    private List<String> dividirCamposSelect(String selectClause) {
        List<String> campos = new ArrayList<>();
        StringBuilder campoActual = new StringBuilder();
        int nivelParentesis = 0;
        boolean enComillas = false;
        char tipoComilla = 0;

        for (char c : selectClause.toCharArray()) {
            if (!enComillas && (c == '\'' || c == '"')) {
                enComillas = true;
                tipoComilla = c;
            } else if (enComillas && c == tipoComilla) {
                enComillas = false;
            } else if (!enComillas) {
                if (c == '(') nivelParentesis++;
                else if (c == ')') nivelParentesis--;
                else if (c == ',' && nivelParentesis == 0) {
                    campos.add(campoActual.toString().trim());
                    campoActual = new StringBuilder();
                    continue;
                }
            }
            campoActual.append(c);
        }

        if (campoActual.length() > 0) {
            campos.add(campoActual.toString().trim());
        }

        return campos;
    }

    // ==================== APLICAR PAGINACIÓN SEGÚN ESTRATEGIA ====================

    /**
     * Aplica la paginación según la estrategia determinada
     */
    private String aplicarPaginacionSegunEstrategia(String sql, AnalisisPaginacion analisis) {
        EstrategiaPaginacion estrategia = analisis.getEstrategiaPaginacion();

        switch (estrategia) {
            case KEYSET_CON_ID:
                return aplicarKeysetConId(sql, analisis.getCamposDisponibles());

            case KEY_COMPUESTO:
                return aplicarKeysetCompuesto(sql, analisis.getCamposDisponibles());

            // AGREGAR ESTE CASE:
            case KEYSET_CONSOLIDADO:
                return aplicarKeysetConsolidacion(sql, analisis.getCamposDisponibles());

            case OFFSET:
                return aplicarOffset(sql);

            case FALLBACK_LIMIT_ONLY:
                return aplicarLimitSimple(sql);

            case SIN_PAGINACION:
                return aplicarLimitParaConsolidacion(sql);

            default:
                log.warn("Estrategia desconocida, aplicando LIMIT simple");
                return aplicarLimitSimple(sql);
        }
    }

    private String aplicarKeysetConsolidacion(String sql, List<CampoKeyset> campos) {
        if (campos.isEmpty()) {
            log.warn("Sin campos para keyset consolidación");
            return aplicarLimitParaConsolidacion(sql);
        }

        log.info("Aplicando KEYSET_CONSOLIDACION con {} campos: {}",
                campos.size(),
                campos.stream().map(CampoKeyset::getNombreCampo).collect(Collectors.toList()));

        // Construir condición WHERE para keyset
        StringBuilder keysetCondition = new StringBuilder();
        String primerCampo = campos.get(0).getNombreCampo();

        // Usar nombres de parámetros genéricos: keyset_campo_0, keyset_campo_1, etc.
        keysetCondition.append("  AND (:keyset_campo_0 IS NULL OR \n");
        keysetCondition.append("       (").append(primerCampo)
                .append(" > :keyset_campo_0");

        // Agregar campos adicionales en cascada (hasta 3)
        for (int i = 1; i < Math.min(campos.size(), 3); i++) {
            String campo = campos.get(i).getNombreCampo();
            keysetCondition.append(" OR \n        (");

            // Igualdad en campos anteriores
            for (int j = 0; j < i; j++) {
                String campoAnterior = campos.get(j).getNombreCampo();
                keysetCondition.append(campoAnterior)
                        .append(" = :keyset_campo_").append(j)
                        .append(" AND ");
            }

            // Mayor en campo actual
            keysetCondition.append(campo)
                    .append(" > :keyset_campo_").append(i)
                    .append(")");
        }

        keysetCondition.append(")\n");

        // Insertar condición ANTES de GROUP BY
        sql = insertarCondicionKeyset(sql, keysetCondition.toString());

        // Agregar ORDER BY
        StringBuilder orderBy = new StringBuilder("\nORDER BY ");
        for (int i = 0; i < Math.min(campos.size(), 3); i++) {
            if (i > 0) orderBy.append(", ");
            orderBy.append(campos.get(i).getNombreCampo()).append(" ASC");
        }
        sql = insertarOrderBy(sql, orderBy.toString());

        // Agregar LIMIT
        sql = agregarLimitSiNoExiste(sql);

        return sql;
    }

    // ==================== KEYSET CON ID ====================

    /**
     * Aplica paginación KEYSET_CON_ID: i.id + campos adicionales
     */
    private String aplicarKeysetConId(String sql, List<CampoKeyset> campos) {
        log.debug("Aplicando KEYSET_CON_ID con {} campos adicionales", campos.size());

        // Construir condición keyset: i.id + hasta 3 campos adicionales
        StringBuilder keysetCondition = new StringBuilder();
        keysetCondition.append("  AND (:lastId::INTEGER IS NULL OR \n");
        keysetCondition.append("       (i.id > :lastId::INTEGER");

        // Limitar a 3 campos adicionales
        int maxCampos = Math.min(3, campos.size());

        // Agregar condiciones en cascada para cada campo
        for (int i = 0; i < maxCampos; i++) {
            CampoKeyset campo = campos.get(i);
            keysetCondition.append(" OR \n");
            keysetCondition.append("        (i.id = :lastId::INTEGER");

            // Agregar condiciones de campos anteriores (igualdad)
            for (int j = 0; j < i; j++) {
                CampoKeyset campoAnterior = campos.get(j);
                keysetCondition.append(" AND ")
                        .append(campoAnterior.getNombreCampo())
                        .append(" = ")
                        .append(campoAnterior.getParametroJdbc());
            }

            // Agregar condición del campo actual (mayor que)
            keysetCondition.append(" AND ")
                    .append(campo.getNombreCampo())
                    .append(" > ")
                    .append(campo.getParametroJdbc())
                    .append(")");
        }

        keysetCondition.append("))\n");

        // Insertar condición
        sql = insertarCondicionKeyset(sql, keysetCondition.toString());

        // Agregar ORDER BY
        sql = agregarOrderByKeysetConId(sql, campos, maxCampos);

        // Agregar LIMIT (verificar que no exista)
        sql = agregarLimitSiNoExiste(sql);

        return sql;
    }

    private String agregarOrderByKeysetConId(String sql, List<CampoKeyset> campos, int maxCampos) {
        StringBuilder orderBy = new StringBuilder("\nORDER BY i.id ASC");

        for (int i = 0; i < maxCampos; i++) {
            orderBy.append(", ").append(campos.get(i).getNombreCampo()).append(" ASC");
        }

        return insertarOrderBy(sql, orderBy.toString());
    }

    // ==================== KEYSET COMPUESTO ====================

    /**
     * Aplica paginación KEYSET_COMPUESTO: serie_equipo, tipo, fecha, etc.
     */
    private String aplicarKeysetCompuesto(String sql, List<CampoKeyset> campos) {
        log.debug("Aplicando KEYSET_COMPUESTO con {} campos", campos.size());

        if (campos.isEmpty()) {
            log.warn("No hay campos disponibles para keyset compuesto, usando LIMIT simple");
            return aplicarLimitSimple(sql);
        }

        // Limitar a 4 campos para keyset compuesto
        int maxCampos = Math.min(4, campos.size());

        // Construir condición keyset en cascada
        StringBuilder keysetCondition = new StringBuilder();
        keysetCondition.append("  AND (");

        for (int i = 0; i < maxCampos; i++) {
            if (i > 0) {
                keysetCondition.append(" OR \n       ");
            }

            CampoKeyset campo = campos.get(i);

            // Condición: campos anteriores iguales Y campo actual mayor
            keysetCondition.append("(");

            // Agregar igualdades de campos anteriores
            for (int j = 0; j < i; j++) {
                CampoKeyset campoAnterior = campos.get(j);
                keysetCondition.append(campoAnterior.getNombreCampo())
                        .append(" = ")
                        .append(campoAnterior.getParametroJdbc())
                        .append(" AND ");
            }

            // Agregar comparación del campo actual
            keysetCondition.append(campo.getNombreCampo())
                    .append(" > ")
                    .append(campo.getParametroJdbc())
                    .append(")");
        }

        keysetCondition.append(")\n");

        // Insertar condición
        sql = insertarCondicionKeyset(sql, keysetCondition.toString());

        // Agregar ORDER BY
        sql = agregarOrderByKeysetCompuesto(sql, campos, maxCampos);

        // Agregar LIMIT (verificar que no exista)
        sql = agregarLimitSiNoExiste(sql);

        return sql;
    }

    private String agregarOrderByKeysetCompuesto(String sql, List<CampoKeyset> campos, int maxCampos) {
        StringBuilder orderBy = new StringBuilder("\nORDER BY ");

        for (int i = 0; i < maxCampos; i++) {
            if (i > 0) orderBy.append(", ");
            orderBy.append(campos.get(i).getNombreCampo()).append(" ASC");
        }

        return insertarOrderBy(sql, orderBy.toString());
    }

    // ==================== OTRAS ESTRATEGIAS ====================

    /**
     * Aplica paginación OFFSET tradicional
     */
    private String aplicarOffset(String sql) {
        log.debug("Aplicando paginación OFFSET");

        sql = agregarLimitSiNoExiste(sql);

        if (!sql.toUpperCase().contains("OFFSET")) {
            sql += "\nOFFSET COALESCE(:offset::INTEGER, 0)";
        }

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

        // Insertar ANTES de GROUP BY
        if (upper.contains("GROUP BY")) {
            return sql.replaceFirst("(?i)\\s+GROUP\\s+BY", "\n" + condicion + "GROUP BY");
        }
        // Si no hay GROUP BY, insertar antes de ORDER BY
        else if (upper.contains("ORDER BY")) {
            return sql.replaceFirst("(?i)\\s+ORDER\\s+BY", "\n" + condicion + "ORDER BY");
        }
        // Si no hay nada, agregar al final
        else {
            return sql + "\n" + condicion;
        }
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
                // Soporta: campo BETWEEN 'fecha1' AND 'fecha2'
                //          campo BETWEEN DATE 'fecha1' AND DATE 'fecha2'
                //          campo BETWEEN TO_DATE(...) AND TO_DATE(...)
                sql = sql.replaceAll(
                        "(?i)" + escapado + "\\s+BETWEEN\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))\\s+AND\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))",
                        ""
                );

                // 2. Remover comparaciones simples con fechas literales
                // Soporta: campo >= 'fecha', campo > DATE 'fecha', etc.
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