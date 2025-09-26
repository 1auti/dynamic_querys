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

/**
 * Componente que construye queries SQL dinámicamente:
 * - Analiza el WHERE para detectar filtros existentes
 * - Reemplaza filtros hardcodeados por parámetros dinámicos
 * - Agrega paginación automática
 *
 *
 */
@Component
@Slf4j
public class DynamicBuilderQuery {

    // =============== ENUMS ===============

    public enum TipoFiltroDetectado {
        FECHA, ESTADO, PROVINCIA, MUNICIPIO, TIPO_INFRACCION, EXPORTA_SACIT, TIENE_EMAIL
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
        private final Map<TipoFiltroDetectado, String> camposPorFiltro; // ESTADO -> "i.id_estado"

        public boolean tiene(TipoFiltroDetectado tipo) {
            return filtrosDetectados.contains(tipo);
        }

        public String getCampo(TipoFiltroDetectado tipo) {
            return camposPorFiltro.get(tipo);
        }
    }

    // =============== MÉTODO PRINCIPAL ===============

    /**
     * Construye SQL inteligente analizando el WHERE existente
     * NO necesita DTO
     */
    public String construirSqlInteligente(String sqlOriginal) {
        log.debug("🔍 Iniciando construcción inteligente de SQL");

        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        String sql = sqlOriginal.trim();

        // 1. Analizar SELECT para detectar campos disponibles
        AnalisisSQL analisisSelect = analizarEstructuraSQL(sql);

        // 2. Analizar WHERE para detectar filtros existentes
        AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sql);

        // 3. Remover filtros hardcodeados del WHERE
        sql = removerFiltrosHardcodeados(sql, analisisFiltros);

        // 4. Agregar filtros dinámicos parametrizados
        sql = agregarFiltrosDinamicos(sql, analisisSelect, analisisFiltros);

        // 5. Agregar paginación
        sql = agregarPaginacion(sql);

        log.debug("✅ SQL inteligente construido exitosamente");
        log.trace("SQL resultante:\n{}", sql);

        return sql;
    }

    // =============== ANÁLISIS DE SELECT ===============

    private AnalisisSQL analizarEstructuraSQL(String sql) {
        Set<String> camposFecha = new HashSet<String>();
        Set<String> camposUbicacion = new HashSet<String>();
        Set<String> camposCategoricos = new HashSet<String>();
        Set<String> camposBooleanos = new HashSet<String>();
        Map<String, String> nombreCompleto = new HashMap<String, String>();

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

                log.trace("Campo SELECT detectado: {} -> {} ({})", campoSimple, nombreCompletoStr, tipo);
            }
        }

        log.debug("📊 Análisis SELECT: {} campos detectados", nombreCompleto.size());
        return new AnalisisSQL(camposFecha, camposUbicacion, camposCategoricos,
                camposBooleanos, nombreCompleto, tieneWhere);
    }

    // =============== ANÁLISIS DE WHERE ===============

    /**
     * Analiza el WHERE para detectar qué filtros tiene
     */
    private AnalisisFiltros analizarFiltrosExistentes(String sql) {
        Set<TipoFiltroDetectado> filtrosDetectados = new HashSet<TipoFiltroDetectado>();
        Map<TipoFiltroDetectado, String> camposPorFiltro = new HashMap<TipoFiltroDetectado, String>();

        // Extraer WHERE
        Pattern wherePattern = Pattern.compile(
                "WHERE\\s+(.+?)(?:ORDER BY|GROUP BY|LIMIT|OFFSET|$)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher whereMatcher = wherePattern.matcher(sql);

        if (!whereMatcher.find()) {
            log.debug("⚠️ SQL sin WHERE - no hay filtros que reemplazar");
            return new AnalisisFiltros(filtrosDetectados, camposPorFiltro);
        }

        String whereClause = whereMatcher.group(1);
        log.debug("🔎 WHERE encontrado: {}", whereClause.substring(0, Math.min(100, whereClause.length())));

        // Detectar cada tipo de filtro
        detectarFiltroFecha(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroEstado(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroProvincia(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroMunicipio(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroTipoInfraccion(whereClause, filtrosDetectados, camposPorFiltro);
        detectarFiltroExportaSacit(whereClause, filtrosDetectados, camposPorFiltro);

        log.info("🎯 Filtros detectados en WHERE: {}", filtrosDetectados);
        return new AnalisisFiltros(filtrosDetectados, camposPorFiltro);
    }

    private void detectarFiltroFecha(String whereClause, Set<TipoFiltroDetectado> filtros,
                                     Map<TipoFiltroDetectado, String> campos) {
        // Buscar: i.fecha_infraccion >= '2025-03-17'
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|<=|>|<|=|BETWEEN)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.FECHA);
            campos.put(TipoFiltroDetectado.FECHA, campo);
            log.debug("   ✓ Filtro FECHA: {}", campo);
        }
    }

    private void detectarFiltroEstado(String whereClause, Set<TipoFiltroDetectado> filtros,
                                      Map<TipoFiltroDetectado, String> campos) {
        // Buscar: i.id_estado NOT IN (1,310,316) o estado = 5
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*estado[a-zA-Z_]*)\\s*(IN|=|NOT IN)",
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

    private void detectarFiltroProvincia(String whereClause, Set<TipoFiltroDetectado> filtros,
                                         Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.(provincia|contexto))\\s*(IN|=|LIKE)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.PROVINCIA);
            campos.put(TipoFiltroDetectado.PROVINCIA, campo);
            log.debug("   ✓ Filtro PROVINCIA: {}", campo);
        }
    }

    private void detectarFiltroMunicipio(String whereClause, Set<TipoFiltroDetectado> filtros,
                                         Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.municipio)\\s*(IN|=|LIKE)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = patron.matcher(whereClause);
        if (matcher.find()) {
            String campo = matcher.group(1);
            filtros.add(TipoFiltroDetectado.MUNICIPIO);
            campos.put(TipoFiltroDetectado.MUNICIPIO, campo);
            log.debug("   ✓ Filtro MUNICIPIO: {}", campo);
        }
    }

    private void detectarFiltroTipoInfraccion(String whereClause, Set<TipoFiltroDetectado> filtros,
                                              Map<TipoFiltroDetectado, String> campos) {
        Pattern patron = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*tipo[a-zA-Z_]*infra[a-zA-Z_]*)\\s*(IN|=)",
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
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.exporta_sacit)\\s*=",
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

    // =============== REMOVER FILTROS HARDCODEADOS ===============

    private String removerFiltrosHardcodeados(String sql, AnalisisFiltros analisis) {
        String sqlModificado = sql;

        for (TipoFiltroDetectado tipo : analisis.getFiltrosDetectados()) {
            sqlModificado = removerFiltroEspecifico(sqlModificado, tipo, analisis.getCampo(tipo));
        }

        return limpiarWhereVacio(sqlModificado);
    }

    private String removerFiltroEspecifico(String sql, TipoFiltroDetectado tipo, String campo) {
        String campoEscapado = Pattern.quote(campo);
        Pattern patron = null;

        switch (tipo) {
            case FECHA:
                // Remover: i.fecha_infraccion >= '2025-03-17'
                patron = Pattern.compile(
                        campoEscapado + "\\s*(>=|<=|>|<|=|BETWEEN)\\s*'[^']+'",
                        Pattern.CASE_INSENSITIVE
                );
                break;

            case ESTADO:
            case TIPO_INFRACCION:
                // Remover: i.id_estado NOT IN (1,310,316)
                patron = Pattern.compile(
                        campoEscapado + "\\s*(NOT\\s+)?IN\\s*\\([^)]+\\)|" + campoEscapado + "\\s*=\\s*\\d+",
                        Pattern.CASE_INSENSITIVE
                );
                break;

            case PROVINCIA:
            case MUNICIPIO:
                // Remover: c.provincia = 'Buenos Aires'
                patron = Pattern.compile(
                        campoEscapado + "\\s*(=|LIKE|IN)\\s*('[^']+'|\\([^)]+\\))",
                        Pattern.CASE_INSENSITIVE
                );
                break;

            case EXPORTA_SACIT:
                // Remover: elh.exporta_sacit = true
                patron = Pattern.compile(
                        campoEscapado + "\\s*=\\s*(true|false|1|0)",
                        Pattern.CASE_INSENSITIVE
                );
                break;
        }

        if (patron != null) {
            Matcher matcher = patron.matcher(sql);
            if (matcher.find()) {
                String condicion = matcher.group(0);
                sql = removerCondicionWhere(sql, condicion);
                log.debug("🗑️ Filtro {} removido: {}", tipo, condicion);
            }
        }

        return sql;
    }

    private String removerCondicionWhere(String sql, String condicion) {
        String escapada = Pattern.quote(condicion);
        String[] patrones = {
                "\\s*AND\\s+" + escapada + "\\s*",
                "\\s*OR\\s+" + escapada + "\\s*",
                escapada + "\\s*AND\\s+",
                escapada + "\\s*OR\\s+",
                escapada
        };

        for (String patron : patrones) {
            sql = sql.replaceAll(patron, " ");
        }

        return sql;
    }

    private String limpiarWhereVacio(String sql) {
        sql = sql.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");
        sql = sql.replaceAll("(?i)WHERE\\s*$", "");
        sql = sql.replaceAll("(?i)WHERE\\s+(ORDER BY|GROUP BY|LIMIT|OFFSET)", "$1");
        return sql.trim();
    }

    // =============== AGREGAR FILTROS DINÁMICOS ===============

    private String agregarFiltrosDinamicos(String sql, AnalisisSQL analisisSelect,
                                           AnalisisFiltros analisisFiltros) {
        StringBuilder filtros = new StringBuilder();
        boolean tieneWhere = sql.toUpperCase().contains("WHERE");

        // Agregar filtros solo si fueron detectados
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

        if (analisisFiltros.tiene(TipoFiltroDetectado.PROVINCIA)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.PROVINCIA);
            agregarFiltroProvincia(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.MUNICIPIO)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.MUNICIPIO);
            agregarFiltroMunicipio(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.TIPO_INFRACCION)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.TIPO_INFRACCION);
            agregarFiltroTipoInfraccion(filtros, tieneWhere, campo);
            tieneWhere = true;
        }

        if (analisisFiltros.tiene(TipoFiltroDetectado.EXPORTA_SACIT)) {
            String campo = analisisFiltros.getCampo(TipoFiltroDetectado.EXPORTA_SACIT);
            agregarFiltroExportaSacit(filtros, tieneWhere, campo);
        }

        return sql + filtros.toString();
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

        log.debug("➕ Filtro FECHA dinámico: {}", campo);
    }

    private void agregarFiltroEstado(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");

        log.debug("➕ Filtro ESTADO dinámico: {}", campo);
    }

    private void agregarFiltroProvincia(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:provincias::TEXT[] IS NULL OR ")
                .append(campo).append(" = ANY(:provincias::TEXT[]))");

        log.debug("➕ Filtro PROVINCIA dinámico: {}", campo);
    }

    private void agregarFiltroMunicipio(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:municipios::TEXT[] IS NULL OR ")
                .append(campo).append(" = ANY(:municipios::TEXT[]))");

        log.debug("➕ Filtro MUNICIPIO dinámico: {}", campo);
    }

    private void agregarFiltroTipoInfraccion(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:tiposInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:tiposInfracciones::INTEGER[]))");

        log.debug("➕ Filtro TIPO_INFRACCION dinámico: {}", campo);
    }

    private void agregarFiltroExportaSacit(StringBuilder filtros, boolean tieneWhere, String campo) {
        if (!tieneWhere) {
            filtros.append("\nWHERE ");
        } else {
            filtros.append("\n  AND ");
        }

        filtros.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");

        log.debug("➕ Filtro EXPORTA_SACIT dinámico: {}", campo);
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
}