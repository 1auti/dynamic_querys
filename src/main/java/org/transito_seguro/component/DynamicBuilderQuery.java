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

    // =============== MÉTODO PRINCIPAL MEJORADO ===============

    public String construirSqlInteligente(String sqlOriginal) {
        log.debug("🔍 Iniciando construcción inteligente de SQL");

        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        // PASO 1: Limpiar comentarios
        String sqlLimpio = limpiarComentariosSQL(sqlOriginal);

        // PASO 2: Remover ; temporal
        sqlLimpio = removerTerminadoresTemporalmente(sqlLimpio);

        // PASO 3: Analizar
        AnalisisSQL analisisSelect = analizarEstructuraSQL(sqlLimpio);
        AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sqlLimpio);

        // PASO 4: Remover filtros hardcodeados
        sqlLimpio = removerFiltrosHardcodeados(sqlLimpio, analisisFiltros);

        // PASO 5: Agregar filtros dinámicos EN LA POSICIÓN CORRECTA
        sqlLimpio = agregarFiltrosDinamicos(sqlLimpio, analisisSelect, analisisFiltros);

        // PASO 6: Agregar paginación AL FINAL
        sqlLimpio = agregarPaginacion(sqlLimpio);

        log.debug("✅ SQL inteligente construido");
        return sqlLimpio;
    }

    // =============== LIMPIEZA DE COMENTARIOS MEJORADA ===============

    /**
     * Limpia comentarios SQL de forma robusta
     */
    private String limpiarComentariosSQL(String sql) {
        String resultado = sql;

        // 1. Remover comentarios de línea (--) preservando saltos de línea
        resultado = resultado.replaceAll("--[^\n\r]*", "");

        // 2. Remover comentarios de bloque (/* */)
        resultado = resultado.replaceAll("/\\*.*?\\*/", "");

        // 3. Normalizar múltiples espacios/saltos a un solo espacio
        resultado = resultado.replaceAll("\\s+", " ");

        // 4. Limpiar AND/OR huérfanos tras remover comentarios
        resultado = resultado.replaceAll("(?i)\\s+(AND|OR)\\s+(AND|OR)\\s+", " $2 ");
        resultado = resultado.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");

        resultado = resultado.trim();
        log.trace("SQL limpio: {}", resultado.substring(0, Math.min(200, resultado.length())));
        return resultado;
    }

    // =============== ANÁLISIS DE FILTROS MEJORADO ===============

    private AnalisisFiltros analizarFiltrosExistentes(String sql) {
        Set<TipoFiltroDetectado> filtrosDetectados = new HashSet<>();
        Map<TipoFiltroDetectado, String> camposPorFiltro = new HashMap<>();

        // Extraer WHERE
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

        // Detectar filtros específicos para tus queries
        detectarFiltroFecha(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroEstado(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroConcesion(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroExportaSacit(whereClause, filtrosDetectados, camposPorFiltro);

        log.info("🎯 Filtros detectados: {}", filtrosDetectados);
        return new AnalisisFiltros(filtrosDetectados, camposPorFiltro);
    }

    // =============== DETECTORES DE FILTROS ESPECÍFICOS ===============

    private void detectarFiltroFecha(String whereClause, Set<TipoFiltroDetectado> filtros,
                                     Map<TipoFiltroDetectado, String> campos) {
        // Patrones comunes en tus queries
        List<Pattern> patronesFecha = Arrays.asList(
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|<=|>|<|=)\\s*'[^']+'", Pattern.CASE_INSENSITIVE),
                Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*>\\s*now\\(\\)", Pattern.CASE_INSENSITIVE)
        );

        for (Pattern patron : patronesFecha) {
            Matcher matcher = patron.matcher(whereClause);
            if (matcher.find()) {
                String campo = matcher.group(1);
                filtros.add(TipoFiltroDetectado.FECHA);
                campos.put(TipoFiltroDetectado.FECHA, campo);
                log.debug("   ✓ Filtro FECHA: {}", campo);
                break; // Solo el primero
            }
        }
    }

    private void detectarFiltroEstado(String whereClause, Set<TipoFiltroDetectado> filtros,
                                      Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.id_estado)\\s*(=|IN)\\s*\\(?([0-9,\\s]+)\\)?",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
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

    // =============== REMOCIÓN DE FILTROS MEJORADA ===============

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
                // Remover fechas con todos los operadores posibles
                sql = sql.replaceAll(
                        campoEscapado + "\\s*(>=|<=|>|<|=|BETWEEN)\\s*'[^']*'(\\s*AND\\s*'[^']*')?",
                        ""
                );
                sql = sql.replaceAll(
                        campoEscapado + "\\s*>\\s*now\\(\\)\\s*-\\s*interval\\s*'[^']*'",
                        ""
                );
                break;

            case ESTADO:
            case CONCESION:
                sql = sql.replaceAll(
                        campoEscapado + "\\s*(=|IN)\\s*\\(?[0-9,\\s]+\\)?",
                        ""
                );
                break;

            case EXPORTA_SACIT:
                sql = sql.replaceAll(
                        campoEscapado + "\\s*=\\s*(true|false)",
                        ""
                );
                break;
        }

        log.debug("🗑️ Filtro {} removido", tipo);
        return sql;
    }

    private String limpiarWhereVacio(String sql) {
        // Múltiples pasadas para casos complejos
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

        // ✅ NUEVO: Identificar dónde termina el WHERE
        String sqlSinTerminadores = removerTerminadoresTemporalmente(sql);

        StringBuilder filtros = new StringBuilder();
        boolean tieneWhere = sqlSinTerminadores.toUpperCase().contains("WHERE");

        if (analisisFiltros.tiene(TipoFiltroDetectado.FECHA)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.FECHA);
            agregarFiltroFecha(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.ESTADO)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.ESTADO);
            agregarFiltroEstado(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.CONCESION)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.CONCESION);
            agregarFiltroConcesion(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.EXPORTA_SACIT)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.EXPORTA_SACIT);
            agregarFiltroExportaSacit(filtros, tieneWhere, campo);
        }

        return insertarFiltrosEnPosicionCorrecta(sqlSinTerminadores, filtros.toString());
    }

    private String removerTerminadoresTemporalmente(String sql) {
        return sql.replaceAll(";\\s*$", "").trim();
    }

    /**
     * ✅ NUEVO: Inserta filtros ANTES de GROUP BY/ORDER BY/LIMIT
     */
    private String insertarFiltrosEnPosicionCorrecta(String sql, String filtrosDinamicos) {
        if (filtrosDinamicos.isEmpty()) {
            return sql;
        }

        // Buscar dónde insertar (antes de GROUP BY, ORDER BY, LIMIT, HAVING)
        Pattern patron = Pattern.compile(
                "(.*?)(\\s+(GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET).*)$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = patron.matcher(sql);

        if (matcher.matches()) {
            // Insertar entre WHERE y GROUP BY/ORDER BY/etc
            String parteAntes = matcher.group(1).trim();
            String parteDespues = matcher.group(2);

            return parteAntes + "\n" + filtrosDinamicos + "\n" + parteDespues;
        } else {
            // No hay GROUP BY/ORDER BY, agregar al final
            return sql + "\n" + filtrosDinamicos;
        }
    }

    private void agregarFiltroFecha(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

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
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");
    }

    private void agregarFiltroConcesion(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:concesiones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:concesiones::INTEGER[]))");
    }

    private void agregarFiltroExportaSacit(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");
    }

    // =============== PAGINACIÓN ===============

    private String agregarPaginacion(String sql) {
        String sqlUpper = sql.toUpperCase();

        if (!sqlUpper.contains("LIMIT")) {
            sql += "\nLIMIT COALESCE(:limite::INTEGER, 1000)";
        }

        if (!sqlUpper.contains("OFFSET")) {
            sql += "\nOFFSET COALESCE(:offset::INTEGER, 0)";
        }

        return sql;
    }

    // =============== ANÁLISIS SELECT (MANTENER ORIGINAL) ===============

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