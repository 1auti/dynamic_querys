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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Constructor dinámico de queries SQL con paginación inteligente.
 * Aplica filtros dinámicos y estrategias de paginación optimizadas.
 *
 * @author Transito Seguro Team
 * @version 2.0 - Corregido y mejorado
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DynamicBuilderQuery {

    private final PaginationStrategyAnalyzer paginationStrategyAnalyzer;

    /**
     * Mapa temporal para proteger subconsultas durante el procesamiento.
     */
    private Map<String, String> subconsultasProtegidas;

    // ==================== CONSTANTES ====================

    /**
     * Límite por defecto cuando no se especifica uno.
     */
    private static final int LIMITE_DEFAULT = 1000;

    /**
     * Límite máximo absoluto para prevenir consultas excesivas.
     */
    private static final int LIMITE_MAXIMO = 10000;

    /**
     * Patterns compilados para mejor performance (solo se compilan una vez).
     */
    private static final Pattern WHERE_PATTERN = Pattern.compile(
            "WHERE\\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|;|$)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private static final Pattern FILTRO_FECHA_BETWEEN = Pattern.compile(
            "\\s+BETWEEN\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))\\s+AND\\s+(?:DATE\\s+)?(?:'[^']*'|TO_DATE\\([^)]+\\))",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern FILTRO_FECHA_COMPARACION = Pattern.compile(
            "\\s*(?:>=|>|<=|<|=)\\s*(?:(?:DATE\\s+)?'[^']*'|TO_DATE\\s*\\([^)]+\\)|(?:CAST|CONVERT)\\s*\\([^)]+\\))",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern FILTRO_IN = Pattern.compile(
            "\\s+(?:NOT\\s+)?IN\\s*\\([^)]+\\)",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern FILTRO_EQUALS = Pattern.compile(
            "\\s*=\\s*\\d+",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern FILTRO_BOOLEAN = Pattern.compile(
            "\\s*=\\s*(?:true|false|TRUE|FALSE)",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern CASE_PATTERN = Pattern.compile(
            "\\bCASE\\b",
            Pattern.CASE_INSENSITIVE
    );

    // ==================== INNER CLASS ====================

    /**
     * Análisis de filtros detectados en una query SQL.
     */
    @Getter
    @AllArgsConstructor
    public static class AnalisisFiltros {
        private final Set<TipoFiltroDetectado> filtrosDetectados;
        private final Map<TipoFiltroDetectado, String> camposPorFiltro;

        /**
         * Verifica si un tipo de filtro fue detectado.
         */
        public boolean tiene(TipoFiltroDetectado tipo) {
            return filtrosDetectados.contains(tipo);
        }

        /**
         * Obtiene el nombre del campo para un tipo de filtro.
         */
        public String getCampo(TipoFiltroDetectado tipo) {
            return camposPorFiltro.get(tipo);
        }
    }

    // ==================== MÉTODO PRINCIPAL ====================

    /**
     * Construye SQL inteligente con paginación automática.
     *
     * Proceso:
     * 1. Limpia y protege subconsultas
     * 2. Determina estrategia de paginación
     * 3. Analiza y remueve filtros hardcodeados
     * 4. Agrega filtros dinámicos
     * 5. Aplica paginación según estrategia
     *
     * @param sqlOriginal Query SQL original
     * @return SQL modificado con paginación y filtros dinámicos
     * @throws ValidationException si el SQL es inválido
     */
    public String construirSqlInteligente(String sqlOriginal) {
        log.debug("Iniciando construcción inteligente de SQL");

        // Validación de entrada
        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            throw new ValidationException("SQL original no puede estar vacío");
        }

        try {
            // 1. Limpiar y proteger
            String sql = limpiarComentariosSQL(sqlOriginal);
            sql = removerTerminadoresTemporalmente(sql);
            sql = protegerSubconsultas(sql);

            // 2. Determinar estrategia de paginación
            AnalisisPaginacion analisisPaginacion = paginationStrategyAnalyzer.determinarEstrategia(sql);
            log.info("Estrategia seleccionada: {} - {}",
                    analisisPaginacion.getEstrategiaPaginacion(),
                    analisisPaginacion.getRazon());

            // 3. Analizar y procesar filtros
            AnalisisFiltros analisisFiltros = analizarFiltrosExistentes(sql);
            sql = removerFiltrosHardcodeados(sql, analisisFiltros);
            sql = restaurarSubconsultas(sql);
            sql = agregarFiltrosDinamicos(sql, analisisFiltros);

            // 4. Aplicar paginación según estrategia
            sql = aplicarPaginacionSegunEstrategia(sql, analisisPaginacion);

            log.debug("SQL construido exitosamente: {} caracteres", sql.length());
            return sql;

        } catch (PatternSyntaxException e) {
            log.error("Error en expresión regular durante construcción de SQL", e);
            throw new ValidationException("SQL contiene sintaxis incompatible con el parser");
        } catch (Exception e) {
            log.error("Error inesperado durante construcción de SQL", e);
            throw new ValidationException("Error al procesar SQL: " + e.getMessage());
        }
    }

    // ==================== APLICAR PAGINACIÓN SEGÚN ESTRATEGIA ====================

    /**
     * Aplica la paginación según la estrategia determinada.
     * MEJORADO: Ahora usa un keyset simplificado unificado.
     *
     * @param sql Query SQL a paginar
     * @param analisis Análisis de paginación con estrategia determinada
     * @return SQL con paginación aplicada
     */
    private String aplicarPaginacionSegunEstrategia(String sql, AnalisisPaginacion analisis) {
        EstrategiaPaginacion estrategia = analisis.getEstrategiaPaginacion();

        switch (estrategia) {
            case KEYSET_CON_ID:
            case KEY_COMPUESTO:
            case KEYSET_CONSOLIDADO:
                // UNIFICADO: Todas las estrategias keyset usan el mismo approach simplificado
                return aplicarKeysetSimplificado(sql, analisis.isTieneIdInfracciones());

            case FALLBACK_LIMIT_ONLY:
                return aplicarLimitSimple(sql);

            case SIN_PAGINACION:
                return aplicarLimitParaConsolidacion(sql);

            case OFFSET:
                log.warn("Usando estrategia OFFSET (menos eficiente)");
                return aplicarLimitSimple(sql); // Fallback a LIMIT simple

            default:
                log.warn("Estrategia desconocida: {}, aplicando LIMIT simple", estrategia);
                return aplicarLimitSimple(sql);
        }
    }

    /**
     * Aplica KEYSET SIMPLIFICADO usando solo lastId.
     * MEJORADO: Ahora maneja correctamente el caso con/sin ID.
     *
     * @param sql Query SQL
     * @param tieneId Si la query tiene campo ID disponible
     * @return SQL con keyset aplicado
     */
    private String aplicarKeysetSimplificado(String sql, boolean tieneId) {
        log.debug("🔑 Aplicando KEYSET SIMPLIFICADO (tieneId: {})", tieneId);

        if (tieneId) {
            // Construir condición keyset usando el ID
            // IMPORTANTE: Asumimos que el campo se llama "id" en los resultados
            String keysetCondition = "\n  AND (:lastId::BIGINT IS NULL OR id_infracciones > :lastId::BIGINT)";

            // Insertar condición keyset
            sql = insertarCondicionKeyset(sql, keysetCondition);

            // Agregar ORDER BY por id
            sql = insertarOrderBy(sql, "\nORDER BY id ASC");
        } else {
            log.debug("Sin ID disponible, solo aplicando ORDER BY y LIMIT");
            // Sin ID, solo aplicar ordenamiento básico si hay campos disponibles
            // El ORDER BY debe ser manejado por quien consume la query
        }

        // Agregar LIMIT
        sql = agregarLimitSiNoExiste(sql);

        return sql;
    }

    /**
     * Aplica solo LIMIT sin condiciones keyset.
     * Usado para queries simples o fallback.
     *
     * @param sql Query SQL
     * @return SQL con LIMIT aplicado
     */
    private String aplicarLimitSimple(String sql) {
        log.debug("Aplicando LIMIT simple");
        return agregarLimitSiNoExiste(sql);
    }

    /**
     * Aplica LIMIT para queries consolidadas (GROUP BY).
     *
     * @param sql Query SQL consolidada
     * @return SQL con LIMIT aplicado
     */
    private String aplicarLimitParaConsolidacion(String sql) {
        log.debug("Aplicando LIMIT para query consolidada");
        return agregarLimitSiNoExiste(sql);
    }

    /**
     * Agrega LIMIT solo si no existe ya en el SQL.
     * Usa parámetro :limite con valor por defecto.
     *
     * @param sql Query SQL
     * @return SQL con LIMIT agregado
     */
    private String agregarLimitSiNoExiste(String sql) {
        String upper = sql.toUpperCase();

        // Si ya tiene LIMIT, no agregar otro
        if (upper.contains("LIMIT")) {
            log.debug("LIMIT ya existe, no se agrega duplicado");
            return sql;
        }

        // Agregar LIMIT con valor por defecto y máximo
        return sql + String.format("\nLIMIT LEAST(COALESCE(:limite::INTEGER, %d), %d)",
                LIMITE_DEFAULT, LIMITE_MAXIMO);
    }

    // ==================== UTILIDADES DE INSERCIÓN ====================

    /**
     * Inserta una condición keyset en el SQL.
     * La inserta después del WHERE existente o crea uno nuevo.
     *
     * @param sql Query SQL
     * @param condicion Condición keyset a insertar
     * @return SQL modificado
     */
    private String insertarCondicionKeyset(String sql, String condicion) {
        String upper = sql.toUpperCase();

        // Caso 1: Insertar después del WHERE existente
        if (upper.contains("WHERE")) {
            Pattern wherePattern = Pattern.compile(
                    "(WHERE\\s+.*?)(?=(?:\\s+GROUP BY|\\s+ORDER BY|\\s+LIMIT|$))",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

            Matcher matcher = wherePattern.matcher(sql);
            if (matcher.find()) {
                String whereSection = matcher.group(1);
                String nuevaWhereSection = whereSection + condicion;
                return matcher.replaceFirst(Matcher.quoteReplacement(nuevaWhereSection));
            }
        }

        // Caso 2: No hay WHERE, crear uno después de FROM/JOIN
        Pattern fromPattern = Pattern.compile(
                "(FROM\\s+.*?(?:(?:LEFT|RIGHT|INNER|OUTER)?\\s*JOIN\\s+.*?)*)(?=(?:\\s+WHERE|\\s+GROUP BY|\\s+ORDER BY|\\s+LIMIT|$))",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = fromPattern.matcher(sql);
        if (matcher.find()) {
            String fromSection = matcher.group(1);
            String nuevaSection = fromSection + "\nWHERE 1=1" + condicion;
            return matcher.replaceFirst(Matcher.quoteReplacement(nuevaSection));
        }

        // Fallback: agregar al final antes de GROUP BY/ORDER BY/LIMIT
        log.warn("No se pudo insertar WHERE de forma elegante, usando fallback");
        return sql + "\nWHERE 1=1" + condicion;
    }

    /**
     * Inserta una cláusula ORDER BY en el SQL.
     * La coloca antes de LIMIT o al final.
     *
     * @param sql Query SQL
     * @param orderBy Cláusula ORDER BY a insertar
     * @return SQL modificado
     */
    private String insertarOrderBy(String sql, String orderBy) {
        String upper = sql.toUpperCase();

        // Si ya tiene ORDER BY, no agregar otro
        if (upper.contains("ORDER BY")) {
            log.debug("ORDER BY ya existe, no se agrega duplicado");
            return sql;
        }

        // Insertar antes de LIMIT si existe
        if (upper.contains("LIMIT")) {
            return sql.replaceFirst("(?i)\\s+LIMIT", orderBy + "\nLIMIT");
        }

        // Insertar después de GROUP BY si existe
        if (upper.contains("GROUP BY")) {
            Pattern p = Pattern.compile(
                    "(.*GROUP\\s+BY\\s+[^;]+?)(\\s*(?:HAVING.*?)?)(\\s*$|\\s*;)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
            Matcher m = p.matcher(sql);
            if (m.find()) {
                return m.group(1) + m.group(2) + orderBy + m.group(3);
            }
        }

        // Agregar al final
        return sql + orderBy;
    }

    // ==================== PROTECCIÓN SUBCONSULTAS ====================

    /**
     * Protege subconsultas y expresiones complejas del procesamiento.
     * Las reemplaza temporalmente con placeholders.
     *
     * @param sql Query SQL
     * @return SQL con subconsultas protegidas
     */
    private String protegerSubconsultas(String sql) {
        subconsultasProtegidas = new HashMap<>();

        try {
            // Proteger expresiones CASE (incluyendo CASE anidados)
            sql = protegerExpresionesCase(sql);

            // Proteger EXISTS
            sql = protegerPatron(sql,
                    "EXISTS\\s*\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                    "EXISTS");

            // Proteger subconsultas SELECT
            sql = protegerPatron(sql,
                    "\\(\\s*SELECT[^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                    "SUBSELECT");

            log.debug("Protegidas {} subconsultas y expresiones", subconsultasProtegidas.size());

        } catch (Exception e) {
            log.error("Error al proteger subconsultas", e);
            // Continuar sin protección (mejor que fallar completamente)
        }

        return sql;
    }

    /**
     * Protege expresiones CASE WHEN ... END respetando anidamiento.
     * MEJORADO: Ahora maneja correctamente CASE anidados.
     *
     * @param sql Query SQL
     * @return SQL con CASE protegidos
     */
    private String protegerExpresionesCase(String sql) {
        Matcher matcher = CASE_PATTERN.matcher(sql);
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
     * Encuentra la posición del END que cierra un CASE.
     * Maneja correctamente CASE anidados.
     *
     * @param sql Query SQL
     * @param inicioDesdeCase Posición después de la palabra CASE
     * @return Posición del END correspondiente, o -1 si no se encuentra
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
                // No se encontró END
                return -1;
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
     * Verifica si una posición es límite de palabra.
     * Previene falsos positivos (ej: "ENDCASE" no es "END").
     *
     * @param sql Query SQL
     * @param inicio Posición de inicio de la palabra
     * @param fin Posición de fin de la palabra
     * @return true si es límite de palabra válido
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

    /**
     * Protege un patrón regex reemplazándolo con placeholders.
     *
     * @param sql Query SQL
     * @param patronStr Patrón regex a buscar
     * @param prefijo Prefijo para el placeholder
     * @return SQL con patrón protegido
     */
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

    /**
     * Restaura las subconsultas protegidas en el SQL.
     *
     * @param sql Query SQL con placeholders
     * @return SQL con subconsultas restauradas
     */
    private String restaurarSubconsultas(String sql) {
        if (subconsultasProtegidas != null && !subconsultasProtegidas.isEmpty()) {
            for (Map.Entry<String, String> entry : subconsultasProtegidas.entrySet()) {
                sql = sql.replace(entry.getKey(), entry.getValue());
            }
            subconsultasProtegidas.clear();
            subconsultasProtegidas = null;
        }
        return sql;
    }

    // ==================== LIMPIEZA ====================

    /**
     * Limpia comentarios y normaliza espacios en blanco del SQL.
     *
     * @param sql Query SQL
     * @return SQL limpio
     */
    private String limpiarComentariosSQL(String sql) {
        return sql.replaceAll("--[^\n\r]*", "")      // Comentarios de línea
                .replaceAll("/\\*.*?\\*/", "")        // Comentarios de bloque
                .replaceAll("\\s+", " ")               // Normalizar espacios
                .trim();
    }

    /**
     * Remueve punto y coma final temporalmente.
     *
     * @param sql Query SQL
     * @return SQL sin terminador
     */
    private String removerTerminadoresTemporalmente(String sql) {
        return sql.replaceAll(";\\s*$", "").trim();
    }

    // ==================== ANÁLISIS FILTROS ====================

    /**
     * Analiza los filtros existentes en la cláusula WHERE.
     *
     * @param sql Query SQL
     * @return Análisis de filtros detectados
     */
    private AnalisisFiltros analizarFiltrosExistentes(String sql) {
        Set<TipoFiltroDetectado> filtros = new HashSet<>();
        Map<TipoFiltroDetectado, String> campos = new HashMap<>();

        Matcher whereMatcher = WHERE_PATTERN.matcher(sql);

        if (whereMatcher.find()) {
            String whereClause = whereMatcher.group(1).trim();
            detectarFiltros(whereClause, filtros, campos);
        }

        log.debug("Filtros detectados: {}", filtros);
        return new AnalisisFiltros(filtros, campos);
    }

    /**
     * Detecta tipos específicos de filtros en la cláusula WHERE.
     * MEJORADO: Usa patterns precompilados para mejor performance.
     *
     * @param where Cláusula WHERE
     * @param filtros Set donde agregar filtros detectados
     * @param campos Map donde agregar campos por tipo de filtro
     */
    private void detectarFiltros(String where, Set<TipoFiltroDetectado> filtros,
                                 Map<TipoFiltroDetectado, String> campos) {

        // Detectar filtro de FECHA
        detectarFiltroFecha(where, filtros, campos);

        // Detectar filtro de ESTADO
        detectarFiltroCampo(where, "id_estado", TipoFiltroDetectado.ESTADO, filtros, campos);

        // Detectar filtro de TIPO_INFRACCION
        detectarFiltroCampo(where, "id_tipo_infra", TipoFiltroDetectado.TIPO_INFRACCION, filtros, campos);

        // Detectar filtro de EXPORTA_SACIT
        detectarFiltroBooleano(where, "exporta_sacit", TipoFiltroDetectado.EXPORTA_SACIT, filtros, campos);
    }

    /**
     * Detecta filtros de fecha (comparaciones y BETWEEN).
     */
    private void detectarFiltroFecha(String where, Set<TipoFiltroDetectado> filtros,
                                     Map<TipoFiltroDetectado, String> campos) {
        Pattern pattern = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=|BETWEEN)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(where);
        if (matcher.find()) {
            filtros.add(TipoFiltroDetectado.FECHA);
            campos.put(TipoFiltroDetectado.FECHA, matcher.group(1));
            log.debug("Filtro FECHA detectado: {}", matcher.group(1));
        }
    }

    /**
     * Detecta filtros de campos con IN/NOT IN.
     */
    private void detectarFiltroCampo(String where, String nombreCampo, TipoFiltroDetectado tipo,
                                     Set<TipoFiltroDetectado> filtros,
                                     Map<TipoFiltroDetectado, String> campos) {
        Pattern pattern = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\." + Pattern.quote(nombreCampo) + ")\\s+(?:NOT\\s+)?IN",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(where);
        if (matcher.find()) {
            filtros.add(tipo);
            campos.put(tipo, matcher.group(1));
            log.debug("Filtro {} detectado: {}", tipo, matcher.group(1));
        }
    }

    /**
     * Detecta filtros booleanos.
     */
    private void detectarFiltroBooleano(String where, String nombreCampo, TipoFiltroDetectado tipo,
                                        Set<TipoFiltroDetectado> filtros,
                                        Map<TipoFiltroDetectado, String> campos) {
        Pattern pattern = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\." + Pattern.quote(nombreCampo) + ")\\s*=",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(where);
        if (matcher.find()) {
            filtros.add(tipo);
            campos.put(tipo, matcher.group(1));
            log.debug("Filtro {} detectado: {}", tipo, matcher.group(1));
        }
    }

    // ==================== REMOVER FILTROS ====================

    /**
     * Remueve filtros hardcodeados del SQL para reemplazarlos con dinámicos.
     *
     * @param sql Query SQL
     * @param analisis Análisis de filtros detectados
     * @return SQL sin filtros hardcodeados
     */
    private String removerFiltrosHardcodeados(String sql, AnalisisFiltros analisis) {
        for (TipoFiltroDetectado tipo : analisis.getFiltrosDetectados()) {
            sql = removerFiltro(sql, tipo, analisis.getCampo(tipo));
        }
        return limpiarWhereVacio(sql);
    }

    /**
     * Remueve un filtro específico de la query SQL
     *
     * @param sql Query SQL original
     * @param tipo Tipo de filtro a remover
     * @param campo Campo del filtro (ej: i.fecha_infraccion)
     * @return SQL sin el filtro especificado
     */
    private String removerFiltro(String sql, TipoFiltroDetectado tipo, String campo) {
        // ✅ SOLUCIÓN: Escapar correctamente para regex SIN usar Pattern.quote
        // Pattern.quote agrega \Q y \E que causan problemas si no se hace replace
        String escapado = escaparParaRegex(campo);

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

            default:
                log.warn("Tipo de filtro no soportado para remoción: {}", tipo);
                break;
        }

        return sql;
    }

    /**
     * Escapa caracteres especiales de regex SIN usar Pattern.quote
     * Esto evita que se agreguen \Q y \E que corrompen la query
     *
     * @param texto Texto a escapar
     * @return Texto con caracteres especiales escapados
     */
    private String escaparParaRegex(String texto) {
        // Escapar solo los caracteres que son especiales en regex
        // NO usar Pattern.quote porque agrega \Q...\E
        return texto.replaceAll("([\\\\.*+?^${}()|\\[\\]])", "\\\\$1");
    }

    /**
     * Limpia WHERE vacío o con operadores huérfanos.
     * Ejecuta múltiples pasadas para limpiar completamente.
     *
     * @param sql Query SQL
     * @return SQL con WHERE limpio
     */
    private String limpiarWhereVacio(String sql) {
        // Múltiples pasadas para limpiar completamente
        for (int i = 0; i < 3; i++) {
            // Remover AND/OR duplicados
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(AND|OR)\\s+", " $2 ");

            // Remover AND/OR al inicio del WHERE
            sql = sql.replaceAll("(?i)WHERE\\s+(AND|OR)\\s+", "WHERE ");

            // Remover AND/OR antes de GROUP BY/ORDER BY/LIMIT
            sql = sql.replaceAll("(?i)\\s+(AND|OR)\\s+(GROUP BY|ORDER BY|LIMIT|$)", " $2");
        }

        // Remover WHERE vacío
        sql = sql.replaceAll("(?i)WHERE\\s+(GROUP BY|ORDER BY|LIMIT|$)", "$1");

        // Normalizar espacios
        return sql.replaceAll("\\s+", " ").trim();
    }

    // ==================== AGREGAR FILTROS DINÁMICOS ====================

    /**
     * Agrega filtros dinámicos parametrizados al SQL.
     *
     * @param sql Query SQL
     * @param analisis Análisis de filtros detectados
     * @return SQL con filtros dinámicos agregados
     */
    private String agregarFiltrosDinamicos(String sql, AnalisisFiltros analisis) {
        boolean tieneWhere = detectarWhere(sql);
        StringBuilder filtros = new StringBuilder();

        // Agregar cada tipo de filtro detectado
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

        // Insertar filtros en el SQL si hay alguno
        if (filtros.length() > 0) {
            return insertarFiltros(sql, filtros.toString());
        }

        return sql;
    }

    /**
     * Detecta si el SQL ya tiene una cláusula WHERE.
     *
     * @param sql Query SQL
     * @return true si tiene WHERE
     */
    private boolean detectarWhere(String sql) {
        String upper = sql.toUpperCase();
        int nivel = 0;

        // Buscar WHERE fuera de paréntesis
        for (int i = 0; i < sql.length() - 5; i++) {
            if (sql.charAt(i) == '(') {
                nivel++;
            } else if (sql.charAt(i) == ')') {
                nivel--;
            }

            if (nivel == 0 && upper.substring(i, Math.min(i + 6, upper.length())).equals("WHERE ")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Inserta filtros dinámicos en la posición correcta del SQL.
     *
     * @param sql Query SQL
     * @param filtros Filtros a insertar
     * @return SQL con filtros insertados
     */
    private String insertarFiltros(String sql, String filtros) {
        Pattern p = Pattern.compile(
                "(.*?)(\\s+(GROUP BY|ORDER BY|LIMIT).*)$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher m = p.matcher(sql);
        if (m.matches()) {
            return m.group(1).trim() + "\n" + filtros + "\n" + m.group(2);
        }

        // Si no hay GROUP BY/ORDER BY/LIMIT, agregar al final
        return sql + "\n" + filtros;
    }

    /**
     * Agrega filtro dinámico de fecha.
     * Soporta fecha específica o rango (inicio/fin).
     */
    private void agregarFiltroFecha(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND (" : "\nWHERE (");
        sb.append("\n    (:fechaEspecifica::DATE IS NULL OR DATE(").append(campo)
                .append(") = :fechaEspecifica::DATE)")
                .append("\n    AND (:fechaInicio::DATE IS NULL OR DATE(").append(campo)
                .append(") >= :fechaInicio::DATE)")
                .append("\n    AND (:fechaFin::DATE IS NULL OR DATE(").append(campo)
                .append(") <= :fechaFin::DATE)\n  )");
    }

    /**
     * Agrega filtro dinámico de estado.
     * Soporta array de IDs de estados.
     */
    private void agregarFiltroEstado(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:estadosInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:estadosInfracciones::INTEGER[]))");
    }

    /**
     * Agrega filtro dinámico de tipo de infracción.
     * Soporta array de IDs de tipos.
     */
    private void agregarFiltroTipoInfraccion(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:tiposInfracciones::INTEGER[] IS NULL OR ")
                .append(campo).append(" = ANY(:tiposInfracciones::INTEGER[]))");
    }

    /**
     * Agrega filtro dinámico de exportado a SACIT.
     * Soporta valor booleano nullable.
     */
    private void agregarFiltroExportaSacit(StringBuilder sb, boolean tieneWhere, String campo) {
        sb.append(tieneWhere ? "\n  AND " : "\nWHERE ");
        sb.append("(:exportadoSacit::BOOLEAN IS NULL OR ")
                .append(campo).append(" = :exportadoSacit::BOOLEAN)");
    }
}