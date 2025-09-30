package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.exception.ValidationException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.enums.TipoCampo.determinarTipoCampo;

@Component
@Slf4j
public class DynamicBuilderQuery {

    // =============== CAMPOS DE INSTANCIA ===============

    private Map<String, String> subconsultasProtegidas;

    // =============== ENUMS ===============

    public enum TipoFiltroDetectado {
        FECHA, ESTADO, PROVINCIA, MUNICIPIO, TIPO_INFRACCION, EXPORTA_SACIT, CONCESION
    }

    // =============== CLASES INTERNAS ===============

    @Getter
    @AllArgsConstructor
    public static class AnalisisSQL {
        private final Set<String> camposFecha;
        private final Set<String> camposUbicacion;
        private final Set<String> camposCategoricos;
        private final Set<String> camposBooleanos;
        private final Map<String, String> nombreCompleto;
        private final boolean tieneWhere;
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
        log.debug("🔍 Iniciando construcción inteligente de SQL");

        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        // PASO 1: Limpiar comentarios
        String sqlLimpio = limpiarComentariosSQL(sqlOriginal);

        // PASO 2: Remover ; temporal
        sqlLimpio = removerTerminadoresTemporalmente(sqlLimpio);

        // PASO 3: Proteger subconsultas
        sqlLimpio = protegerSubconsultas(sqlLimpio);

        // PASO 4: Analizar
        AnalisisSQL analisisSelect = analizarEstructuraSQL(sqlLimpio);
        AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sqlLimpio);

        // PASO 5: Remover filtros hardcodeados
        sqlLimpio = removerFiltrosHardcodeados(sqlLimpio, analisisFiltros);

        // PASO 6: Restaurar subconsultas
        sqlLimpio = restaurarSubconsultas(sqlLimpio);

        // PASO 7: Agregar filtros dinámicos
        sqlLimpio = agregarFiltrosDinamicos(sqlLimpio, analisisSelect, analisisFiltros);

        // PASO 8: Agregar paginación
        sqlLimpio = agregarPaginacion(sqlLimpio);

        log.debug("✅ SQL inteligente construido: {} caracteres", sqlLimpio.length());
        return sqlLimpio;
    }

    // =============== PROTECCIÓN DE SUBCONSULTAS ===============

    private String protegerSubconsultas(String sql) {
        subconsultasProtegidas = new HashMap<>();

        // Proteger EXISTS
        sql = protegerPatron(sql, "EXISTS\\s*\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)", "EXISTS");

        // Proteger SELECT en CASE WHEN
        sql = protegerPatron(sql, "\\(\\s*SELECT[^()]*(?:\\([^()]*\\)[^()]*)*\\)", "SUBSELECT");

        log.debug("📦 Protegidas {} subconsultas", subconsultasProtegidas.size());
        return sql;
    }

    private String protegerPatron(String sql, String patronStr, String prefijo) {
        Pattern patron = Pattern.compile(patronStr, Pattern.CASE_INSENSITIVE);
        Matcher matcher = patron.matcher(sql);

        StringBuffer sb = new StringBuffer();
        int contador = 0;

        while (matcher.find()) {
            String subconsulta = matcher.group(0);
            String placeholder = "___" + prefijo + "_" + contador + "___";
            subconsultasProtegidas.put(placeholder, subconsulta);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(placeholder));
            contador++;
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    private String restaurarSubconsultas(String sql) {
        if (subconsultasProtegidas == null || subconsultasProtegidas.isEmpty()) {
            return sql;
        }

        for (Map.Entry<String, String> entry : subconsultasProtegidas.entrySet()) {
            sql = sql.replace(entry.getKey(), entry.getValue());
        }

        log.debug("🔓 Restauradas {} subconsultas", subconsultasProtegidas.size());
        subconsultasProtegidas = null;
        return sql;
    }

    // =============== LIMPIEZA ===============

    private String limpiarComentariosSQL(String sql) {
        String resultado = sql;
        resultado = resultado.replaceAll("--[^\n\r]*", "");
        resultado = resultado.replaceAll("/\\*.*?\\*/", "");
        resultado = resultado.replaceAll("\\s+", " ");
        resultado = resultado.replaceAll("(?i)\\s+(AND|OR)\\s+(AND|OR)\\s+", " $2 ");
        resultado = resultado.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");
        return resultado.trim();
    }

    private String removerTerminadoresTemporalmente(String sql) {
        return sql.replaceAll(";\\s*$", "").trim();
    }

    // =============== ANÁLISIS DE FILTROS ===============

    private AnalisisFiltros analizarFiltrosExistentes(String sql) {
        Set<TipoFiltroDetectado> filtrosDetectados = new HashSet<>();
        Map<TipoFiltroDetectado, String> camposPorFiltro = new HashMap<>();

        Pattern wherePattern = Pattern.compile(
                "WHERE\\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|OFFSET|;|$)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher whereMatcher = wherePattern.matcher(sql);

        if (!whereMatcher.find()) {
            log.debug("⚠️ SQL sin WHERE");
            return new AnalisisFiltros(filtrosDetectados, camposPorFiltro);
        }

        String whereClause = whereMatcher.group(1).trim();
        log.debug("🔎 WHERE encontrado: {} caracteres", whereClause.length());

        detectarFiltroFecha(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroEstado(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroConcesion(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroTipoInfraccion(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroExportaSacit(whereClause, filtrosDetectados, camposPorFiltro);

        log.info("🎯 Filtros detectados: {}", filtrosDetectados);
        return new AnalisisFiltros(filtrosDetectados, camposPorFiltro);
    }

    // =============== DETECTORES MEJORADOS ===============

    private void detectarFiltroFecha(String whereClause, Set<TipoFiltroDetectado> filtros,
                                     Map<TipoFiltroDetectado, String> campos) {
        Set<String> camposFechaEncontrados = new HashSet<>();

        List<Pattern> patronesFecha = Arrays.asList(
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=|=)\\s*(DATE\\s*)?'[^']+'", Pattern.CASE_INSENSITIVE),
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>)\\s*TO_DATE\\([^)]+\\)", Pattern.CASE_INSENSITIVE),
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(<|<=)\\s*(NOW|CURRENT_DATE|CURRENT_TIMESTAMP)\\(\\)", Pattern.CASE_INSENSITIVE),
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>)\\s*now\\(\\)", Pattern.CASE_INSENSITIVE)
        );

        for (Pattern patron : patronesFecha) {
            Matcher matcher = patron.matcher(whereClause);
            while (matcher.find()) {
                camposFechaEncontrados.add(matcher.group(1));
            }
        }

        if (!camposFechaEncontrados.isEmpty()) {
            String campoPrincipal = camposFechaEncontrados.iterator().next();
            filtros.add(TipoFiltroDetectado.FECHA);
            campos.put(TipoFiltroDetectado.FECHA, campoPrincipal);
            log.debug("   ✓ Filtros FECHA: {} (principal: {})", camposFechaEncontrados, campoPrincipal);
        }
    }

    private void detectarFiltroEstado(String whereClause, Set<TipoFiltroDetectado> filtros,
                                      Map<TipoFiltroDetectado, String> campos) {
        // Priorizar id_estado sobre id_tipo_infra
        Pattern patronEstado = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_estado)\\s+IN\\s*\\([^)]+\\)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patronEstado.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.ESTADO);
            campos.put(TipoFiltroDetectado.ESTADO, campo);
            log.debug("   ✓ Filtro ESTADO: {}", campo);
        }
    }

    private void detectarFiltroConcesion(String whereClause, Set<TipoFiltroDetectado> filtros,
                                         Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_concesion)\\s*(=|IN)\\s*\\(?([0-9,\\s]+)\\)?",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.CONCESION);
            campos.put(TipoFiltroDetectado.CONCESION, campo);
            log.debug("   ✓ Filtro CONCESION: {}", campo);
        }
    }

    private void detectarFiltroTipoInfraccion(String whereClause, Set<TipoFiltroDetectado> filtros,
                                              Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_tipo_infra)\\s+IN\\s*\\([^)]+\\)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.TIPO_INFRACCION);
            campos.put(TipoFiltroDetectado.TIPO_INFRACCION, campo);
            log.debug("   ✓ Filtro TIPO_INFRACCION: {}", campo);
        }
    }

    private void detectarFiltroExportaSacit(String whereClause, Set<TipoFiltroDetectado> filtros,
                                            Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.exporta_sacit)\\s*=\\s*(true|false)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.EXPORTA_SACIT);
            campos.put(TipoFiltroDetectado.EXPORTA_SACIT, campo);
            log.debug("   ✓ Filtro EXPORTA_SACIT: {}", campo);
        }
    }

    // =============== REMOCIÓN DE FILTROS ===============

    private String removerFiltrosHardcodeados(String sql, AnalisisFiltros analisis) {
        String sqlModificado = sql;

        for (TipoFiltroDetectado tipo : analisis.getFiltrosDetectados()) {
            sqlModificado = removerFiltroEspecifico(sqlModificado, tipo, analisis.getCampo(tipo));
        }

        return limpiarWhereVacio(sqlModificado);
    }

    private String removerFiltroEspecifico(String sql, TipoFiltroDetectado tipo, String campo) {
        String campoEscapado = Pattern.quote(campo);

        switch (tipo) {
            case FECHA:
                // Remover TODOS los patrones de fecha
                sql = sql.replaceAll(campoEscapado + "\\s*(>=|>|<|<=|=)\\s*(DATE\\s*)?'[^']*'", "");
                sql = sql.replaceAll(campoEscapado + "\\s*(>=|>)\\s*TO_DATE\\([^)]+\\)", "");
                sql = sql.replaceAll(campoEscapado + "\\s*(<|<=)\\s*(NOW|CURRENT_DATE|CURRENT_TIMESTAMP)\\(\\)", "");
                sql = sql.replaceAll(campoEscapado + "\\s*(>=|>)\\s*now\\(\\)(\\s*-\\s*interval\\s*'[^']*')?", "");

                // Remover OTROS campos de fecha también
                sql = removerOtrasFechasHardcodeadas(sql);
                break;

            case ESTADO:
            case TIPO_INFRACCION:
                sql = sql.replaceAll(campoEscapado + "\\s+IN\\s*\\([^)]+\\)", "");
                break;

            case CONCESION:
                sql = sql.replaceAll(campoEscapado + "\\s*(=|IN)\\s*\\(?[0-9,\\s]+\\)?", "");
                break;

            case EXPORTA_SACIT:
                sql = sql.replaceAll(campoEscapado + "\\s*=\\s*(true|false)", "");
                break;
        }

        log.debug("🗑️ Filtro {} removido", tipo);
        return sql;
    }

    private String removerOtrasFechasHardcodeadas(String sql) {
        String patronGenerico = "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=)\\s*(DATE\\s*'[^']*'|TO_DATE\\([^)]+\\)|NOW\\(\\)|CURRENT_DATE\\(\\)|CURRENT_TIMESTAMP\\(\\)|'[^']*')";
        sql = sql.replaceAll(patronGenerico, "");
        return sql;
    }

    private String limpiarWhereVacio(String sql) {
        for (int i = 0; i < 3; i++) {
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(AND|OR)\\s+", " $2 ");
            sql = sql.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(GROUP BY|ORDER BY|LIMIT|;|$)", " $2");
        }

        sql = sql.replaceAll("(?i)WHERE\\s+(GROUP BY|ORDER BY|LIMIT|;|$)", "$1");
        sql = sql.replaceAll("\\s+", " ").trim();
        return sql;
    }

    // =============== AGREGAR FILTROS DINÁMICOS ===============

    private String agregarFiltrosDinamicos(String sql, AnalisisSQL analisisSelect,
                                           AnalisisFiltros analisisFiltros) {

        // PASO 1: Limpiar SQL
        sql = limpiarSqlAntesDeAgregarFiltros(sql);

        // PASO 2: Detectar WHERE
        boolean tieneWhere = detectarWhereEnSqlPrincipal(sql);

        StringBuilder filtros = new StringBuilder();

        // PASO 3: Construir filtros (igual que antes)
        if (analisisFiltros.tiene(TipoFiltroDetectado.FECHA)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.FECHA);
            agregarFiltroFechaDinamico(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.ESTADO)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.ESTADO);
            agregarFiltroEstadoDinamico(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.TIPO_INFRACCION)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.TIPO_INFRACCION);
            agregarFiltroTipoInfraccionDinamico(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.EXPORTA_SACIT)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.EXPORTA_SACIT);
            agregarFiltroExportaSacitDinamico(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        // 🔧 PASO 4: INSERTAR FILTROS EN LA POSICIÓN CORRECTA
        if (filtros.length() > 0) {
            return insertarFiltrosAntesDeClausulas(sql, filtros.toString());
        }

        return sql;
    }

    /**
     * 🔧 NUEVO: Inserta filtros ANTES de GROUP BY, ORDER BY, LIMIT
     */
    private String insertarFiltrosAntesDeClausulas(String sql, String filtrosDinamicos) {
        // Buscar dónde termina el WHERE/FROM y empiezan las cláusulas finales
        Pattern patron = Pattern.compile(
                "(.*?)(\\s+(GROUP BY|ORDER BY|LIMIT|OFFSET).*)$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = patron.matcher(sql);

        if (matcher.matches()) {
            String parteAntes = matcher.group(1).trim();
            String parteDespues = matcher.group(2);

            log.debug("📍 Insertando filtros ANTES de: {}",
                    parteDespues.substring(0, Math.min(30, parteDespues.length())));

            return parteAntes + "\n" + filtrosDinamicos + "\n" + parteDespues;
        } else {
            // Si no hay cláusulas finales, agregar al final
            log.debug("📍 Agregando filtros AL FINAL (no hay GROUP BY/ORDER BY)");
            return sql + "\n" + filtrosDinamicos;
        }
    }


    /**
     * Limpia SQL de problemas comunes antes de agregar filtros
     */
    private String limpiarSqlAntesDeAgregarFiltros(String sql) {
        // 1. Quitar ANDs colgantes al final
        sql = sql.replaceAll("(?i)\\s+AND\\s*$", "").trim();

        // 2. Corregir "AND WHERE" a solo "WHERE"
        sql = sql.replaceAll("(?i)\\s+AND\\s+WHERE\\s+", " WHERE ");

        // 3. Quitar ORs colgantes (menos común pero puede pasar)
        sql = sql.replaceAll("(?i)\\s+OR\\s*$", "").trim();

        // 4. Limpiar espacios múltiples
        sql = sql.replaceAll("\\s+", " ");

        log.trace("SQL limpio: {}", sql.substring(Math.max(0, sql.length() - 100)));

        return sql;
    }

    /**
     * Detecta si el SQL principal tiene WHERE (ignorando subconsultas)
     */
    private boolean detectarWhereEnSqlPrincipal(String sql) {
        String sqlUpper = sql.toUpperCase();

        // Buscar WHERE que no esté dentro de paréntesis (subconsultas)
        int nivelParentesis = 0;
        int posWhere = -1;

        for (int i = 0; i < sql.length() - 5; i++) {
            char c = sql.charAt(i);

            if (c == '(') nivelParentesis++;
            else if (c == ')') nivelParentesis--;

            // Solo considerar WHERE en nivel 0 (no en subconsultas)
            if (nivelParentesis == 0 &&
                    sqlUpper.substring(i, Math.min(i + 6, sqlUpper.length())).equals("WHERE ")) {
                posWhere = i;
                break;
            }
        }

        boolean tieneWhere = posWhere > 0;
        log.trace("WHERE detectado en SQL principal: {} (posición: {})", tieneWhere, posWhere);

        return tieneWhere;
    }

// =============== MÉTODOS DE AGREGADO DE FILTROS ===============

    /**
     * Agrega filtro de fecha con manejo correcto de WHERE/AND
     */
    private void agregarFiltroFechaDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE (");
        } else {
            filtros.append("\n  AND (");
        }

        filtros.append("\n    (:fechaEspecifica::DATE IS NULL OR DATE(")
                .append(campo).append(") = :fechaEspecifica::DATE)")
                .append("\n    AND (:fechaInicio::DATE IS NULL OR DATE(")
                .append(campo).append(") >= :fechaInicio::DATE)")
                .append("\n    AND (:fechaFin::DATE IS NULL OR DATE(")
                .append(campo).append(") <= :fechaFin::DATE)")
                .append("\n  )");

        log.debug("✅ Filtro FECHA agregado: {} (WHERE: {})", campo, !tieneWhere);
    }

    /**
     * Agrega filtro de estado con manejo correcto de WHERE/AND
     */
    private void agregarFiltroEstadoDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");

        log.debug("✅ Filtro ESTADO agregado: {} (WHERE: {})", campo, !tieneWhere);
    }

    /**
     * Agrega filtro de provincia con manejo correcto de WHERE/AND
     */
    private void agregarFiltroProvinciaDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:provincias::TEXT[] IS NULL OR ")
                .append(campo).append(" = ANY(:provincias::TEXT[]))");

        log.debug("✅ Filtro PROVINCIA agregado: {} (WHERE: {})", campo, !tieneWhere);
    }

    /**
     * Agrega filtro de municipio con manejo correcto de WHERE/AND
     */
    private void agregarFiltroMunicipioDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:municipios::TEXT[] IS NULL OR ")
                .append(campo).append(" = ANY(:municipios::TEXT[]))");

        log.debug("✅ Filtro MUNICIPIO agregado: {} (WHERE: {})", campo, !tieneWhere);
    }

    /**
     * Agrega filtro de tipo infracción con manejo correcto de WHERE/AND
     */
    private void agregarFiltroTipoInfraccionDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:tiposInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:tiposInfracciones::INTEGER[]))");

        log.debug("✅ Filtro TIPO_INFRACCION agregado: {} (WHERE: {})", campo, !tieneWhere);
    }

    /**
     * Agrega filtro de exporta SACIT con manejo correcto de WHERE/AND
     */
    private void agregarFiltroExportaSacitDinamico(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");

        log.debug("✅ Filtro EXPORTA_SACIT agregado: {} (WHERE: {})", campo, !tieneWhere);
    }



    private String insertarFiltrosEnPosicionCorrecta(String sql, String filtrosDinamicos) {
        if (filtrosDinamicos.isEmpty()) {
            return sql;
        }

        Pattern patron = Pattern.compile(
                "(.*?)(\\s+(GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET).*)$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = patron.matcher(sql);

        if (matcher.matches()) {
            String parteAntes = matcher.group(1).trim();
            String parteDespues = matcher.group(2);
            return parteAntes + "\n" + filtrosDinamicos + "\n" + parteDespues;
        } else {
            return sql + "\n" + filtrosDinamicos;
        }
    }

    // Agregar filtros individuales
    private void agregarFiltroFecha(StringBuilder filtros, boolean tieneWhere, String campo) {
        filtros.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        filtros.append("(\n")
                .append("    (:fechaEspecifica::DATE IS NULL OR DATE(").append(campo)
                .append(") = :fechaEspecifica::DATE)\n")
                .append("    AND (:fechaInicio::DATE IS NULL OR DATE(").append(campo)
                .append(") >= :fechaInicio::DATE)\n")
                .append("    AND (:fechaFin::DATE IS NULL OR DATE(").append(campo)
                .append(") <= :fechaFin::DATE)\n")
                .append("  )");
    }

    private void agregarFiltroEstado(StringBuilder filtros, boolean tieneWhere, String campo) {
        filtros.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        filtros.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");
    }

    private void agregarFiltroTipoInfraccion(StringBuilder filtros, boolean tieneWhere, String campo) {
        filtros.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        filtros.append("(:tiposInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:tiposInfracciones::INTEGER[]))");
    }

    private void agregarFiltroConcesion(StringBuilder filtros, boolean tieneWhere, String campo) {
        filtros.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        filtros.append("(:concesiones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:concesiones::INTEGER[]))");
    }

    private void agregarFiltroExportaSacit(StringBuilder filtros, boolean tieneWhere, String campo) {
        filtros.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        filtros.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");
    }

    private String agregarPaginacion(String sql) {
        String sqlUpper = sql.toUpperCase();

        // ✅ NUEVO: Detectar si hay Keyset params
        boolean tieneKeyset = sql.contains(":lastId");

        if (tieneKeyset) {
            // 🚀 KEYSET PAGINATION - agregar WHERE con lastKey
            log.debug("Aplicando Keyset Pagination");

            // Buscar WHERE o agregar uno nuevo
            if (!sqlUpper.contains("WHERE")) {
                sql = sql.replaceFirst("(?i)GROUP BY",
                        "WHERE (:lastId::INTEGER IS NULL OR " +
                                "(e.id, pc.serie_equipo, pc.lugar) > " +
                                "(:lastId::INTEGER, :lastSerieEquipo::TEXT, :lastLugar::TEXT))\n" +
                                "GROUP BY");
            } else {
                sql = sql.replaceFirst("(?i)GROUP BY",
                        "AND (:lastId::INTEGER IS NULL OR " +
                                "(e.id, pc.serie_equipo, pc.lugar) > " +
                                "(:lastId::INTEGER, :lastSerieEquipo::TEXT, :lastLugar::TEXT))\n" +
                                "GROUP BY");
            }

            // Solo LIMIT, sin OFFSET
            if (!sqlUpper.contains("LIMIT")) {
                sql += "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
            }

        } else {
            // 📊 OFFSET TRADICIONAL
            if (!sqlUpper.contains("LIMIT")) {
                sql += "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
            }
            if (!sqlUpper.contains("OFFSET")) {
                sql += "\nOFFSET COALESCE(:offset::INTEGER, 0)";
            }
        }

        return sql;
    }

    // =============== ANÁLISIS SELECT ===============

    private AnalisisSQL analizarEstructuraSQL(String sql) {
        Set<String> camposFecha = new HashSet<>();
        Set<String> camposUbicacion = new HashSet<>();
        Set<String> camposCategoricos = new HashSet<>();
        Set<String> camposBooleanos = new HashSet<>();
        Map<String, String> nombreCompleto = new HashMap<>();

        boolean tieneWhere = sql.toUpperCase().contains("WHERE");

        Pattern selectPattern = Pattern.compile("SELECT\\s+(.+?)\\s+FROM",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher selectMatcher = selectPattern.matcher(sql);

        if (selectMatcher.find()) {
            String selectClause = selectMatcher.group(1);
            Pattern campoPattern = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b");
            Matcher campoMatcher = campoPattern.matcher(selectClause);

            while (campoMatcher.find()) {
                String tabla = campoMatcher.group(1);
                String campo = campoMatcher.group(2);
                String nombreCompletoStr = tabla + "." + campo;
                String campoSimple = campo.toLowerCase();

                nombreCompleto.put(campoSimple, nombreCompletoStr);

                TipoCampo tipo = determinarTipoCampo(campo, nombreCompletoStr);

                switch (tipo) {
                    case TIEMPO:
                        camposFecha.add(campoSimple);
                        break;
                    case UBICACION:
                        camposUbicacion.add(campoSimple);
                        break;
                    case CATEGORIZACION:
                        camposCategoricos.add(campoSimple);
                        break;
                    case CALCULADO:
                        camposBooleanos.add(campoSimple);
                        break;
                }
            }
        }

        log.debug("📊 Análisis SELECT: {} campos detectados", nombreCompleto.size());
        return new AnalisisSQL(camposFecha, camposUbicacion, camposCategoricos,
                camposBooleanos, nombreCompleto, tieneWhere);
    }
}