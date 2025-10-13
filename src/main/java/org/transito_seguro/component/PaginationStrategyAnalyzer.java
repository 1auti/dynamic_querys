package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoDatoKeyset;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.model.CampoKeyset.getPrioridadTipo;

/**
 * Analizador de estrategias de paginación para queries SQL.
 * Determina la mejor estrategia según las características de la query.
 *
 * @author Transito Seguro Team
 * @version 2.0 - Corregido y mejorado
 */
@Component
@Slf4j
public class PaginationStrategyAnalyzer {

    // ==================== CONSTANTES ====================

    /**
     * Campos candidatos para usar en keyset pagination.
     * Ordenados por prioridad de uso.
     */
    private static final List<String> CAMPOS_KEYSET_CANDIDATOS = Arrays.asList(
            "serie_equipo",
            "id_tipo_infra",
            "fecha_infraccion",
            "id_estado",
            "id_punto_control",
            "packedfile"
    );

    /**
     * Pattern para detectar SELECT con FROM.
     * CORREGIDO: Era "s+" ahora es "\\s+"
     */
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            "SELECT\\s+(.*?)\\s+FROM",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Pattern para detectar GROUP BY con cláusula.
     */
    private static final Pattern GROUP_BY_PATTERN = Pattern.compile(
            "\\bGROUP\\s+BY\\s+[^;]+?(?=\\s+(HAVING|ORDER|LIMIT|$)|$)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern para detectar funciones de agregación.
     */
    private static final Pattern AGREGACION_PATTERN = Pattern.compile(
            "\\b(COUNT|SUM|AVG|MIN|MAX)\\s*\\(",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern para detectar i.id o infracciones.id en SELECT.
     * MEJORADO: Ahora captura el alias si existe.
     */
    private static final Pattern ID_INFRACCIONES_PATTERN = Pattern.compile(
            "SELECT\\s+.*?\\b(i\\.id|infracciones\\.id)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?.*?FROM",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Pattern para extraer campos GROUP BY con nombres.
     */
    private static final Pattern GROUP_BY_NOMBRES_PATTERN = Pattern.compile(
            "GROUP\\s+BY\\s+([a-zA-Z_][a-zA-Z0-9_\\.]*(?:\\s*,\\s*[a-zA-Z_][a-zA-Z0-9_\\.]*)*)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern para extraer campos GROUP BY numéricos (1,2,3).
     */
    private static final Pattern GROUP_BY_NUMERICO_PATTERN = Pattern.compile(
            "GROUP\\s+BY\\s+(\\d+(?:\\s*,\\s*\\d+)*)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern para extraer alias de un campo (después de AS).
     */
    private static final Pattern ALIAS_PATTERN = Pattern.compile(
            "\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Límite máximo de longitud de SQL para procesar (prevención de DoS).
     */
    private static final int MAX_SQL_LENGTH = 100_000;

    // ==================== MÉTODO PRINCIPAL ====================

    /**
     * Determina la estrategia de paginación óptima para una query SQL.
     *
     * @param sql Query SQL a analizar
     * @return Análisis con la estrategia recomendada y campos detectados
     * @throws IllegalArgumentException si el SQL es inválido o demasiado largo
     */
    public AnalisisPaginacion determinarEstrategia(String sql) {
        log.debug("Analizando estrategia de paginación para query");

        // Validación de entrada
        validarSQL(sql);

        // 1. Si es consolidada (GROUP BY), analizar keyset basado en GROUP BY
        if (esQueryConsolidada(sql)) {
            return analizarQueryConsolidada(sql);
        }

        // 2. Verificar si tiene id de infracciones y obtener su alias
        String aliasIdInfracciones = detectarAliasIdInfracciones(sql);
        boolean tieneIdInfracciones = aliasIdInfracciones != null;

        // 3. Detectar campos disponibles en el SELECT
        List<CampoKeyset> camposDisponibles = detectarCamposDisponibles(sql);

        // 4. Determinar estrategia según disponibilidad
        return determinarEstrategiaPorCampos(
                tieneIdInfracciones,
                aliasIdInfracciones,
                camposDisponibles
        );
    }

    // ==================== VALIDACIÓN ====================

    /**
     * Valida que el SQL sea procesable.
     *
     * @param sql Query a validar
     * @throws IllegalArgumentException si el SQL es inválido
     */
    private void validarSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL no puede ser nulo o vacío");
        }

        if (sql.length() > MAX_SQL_LENGTH) {
            throw new IllegalArgumentException(
                    String.format("SQL excede el límite máximo de %d caracteres", MAX_SQL_LENGTH)
            );
        }

        // Validación básica de estructura SELECT...FROM
        String upper = sql.toUpperCase();
        if (!upper.contains("SELECT") || !upper.contains("FROM")) {
            throw new IllegalArgumentException("SQL debe contener SELECT...FROM");
        }
    }

    // ==================== DETECCIÓN DE QUERY CONSOLIDADA ====================

    /**
     * Analiza una query consolidada (con GROUP BY).
     *
     * @param sql Query SQL consolidada
     * @return Análisis de paginación para query consolidada
     */
    private AnalisisPaginacion analizarQueryConsolidada(String sql) {
        List<CampoKeyset> camposGroupBy = extraerCamposGroupBy(sql);

        if (camposGroupBy.size() >= 2) {
            log.info("Query consolidada con keyset: {} campos GROUP BY", camposGroupBy.size());
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEY_COMPUESTO,
                    false,
                    camposGroupBy,
                    "Keyset basado en campos GROUP BY"
            );
        } else {
            log.info("Query consolidada sin keyset viable (solo {} campos)", camposGroupBy.size());
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.SIN_PAGINACION,
                    false,
                    Collections.emptyList(),
                    "GROUP BY insuficiente para keyset (mínimo 2 campos requeridos)"
            );
        }
    }

    /**
     * Detecta si la query es consolidada (tiene GROUP BY con agregaciones).
     *
     * @param sql Query SQL a analizar
     * @return true si es query consolidada
     */
    private boolean esQueryConsolidada(String sql) {
        // Limpiar comentarios básicos
        String sqlLimpio = sql.replaceAll("--.*$", "")
                .replaceAll("/\\*.*?\\*/", "");

        boolean tieneGroupBy = GROUP_BY_PATTERN.matcher(sqlLimpio).find();
        boolean tieneAgg = AGREGACION_PATTERN.matcher(sqlLimpio).find();

        log.debug("Query consolidada: GROUP BY={}, Agregación={}", tieneGroupBy, tieneAgg);
        return tieneGroupBy && tieneAgg;
    }

    // ==================== DETECCIÓN DE ID INFRACCIONES ====================

    /**
     * Detecta si la query tiene i.id (ID de infracciones) y devuelve su alias.
     * CORREGIDO: Ahora detecta el alias correcto que se usará en los resultados.
     *
     * @param sql Query SQL a analizar
     * @return Alias del campo ID (puede ser "id", "id_infraccion", etc.) o null
     */
    private String detectarAliasIdInfracciones(String sql) {
        Matcher matcher = ID_INFRACCIONES_PATTERN.matcher(sql);

        if (matcher.find()) {
            String campoCompleto = matcher.group(1); // i.id o infracciones.id
            String aliasExplicito = matcher.group(2); // Alias después de AS (si existe)

            if (aliasExplicito != null && !aliasExplicito.isEmpty()) {
                // Tiene alias explícito: SELECT i.id AS id_infraccion
                log.debug("ID infracciones detectado con alias explícito: {}", aliasExplicito);
                return aliasExplicito;
            } else {
                // Sin alias explícito, usar "id" como default
                log.debug("ID infracciones detectado sin alias, usando 'id' por defecto");
                return "id";
            }
        }

        log.debug("No se detectó ID de infracciones en el SELECT");
        return null;
    }

    // ==================== DETECCIÓN DE CAMPOS DISPONIBLES ====================

    /**
     * Detecta campos disponibles en el SELECT para usar en keyset.
     * CORREGIDO: Pattern regex ahora funciona correctamente.
     *
     * @param sql Query SQL a analizar
     * @return Lista de campos detectados con su metadata
     */
    private List<CampoKeyset> detectarCamposDisponibles(String sql) {
        List<CampoKeyset> campos = new ArrayList<>();
        Set<TipoDatoKeyset> tiposUsados = new HashSet<>();

        // Extraer la cláusula SELECT
        Matcher matcher = SELECT_PATTERN.matcher(sql);

        if (!matcher.find()) {
            log.warn("No se pudo extraer cláusula SELECT");
            return campos;
        }

        String selectClause = matcher.group(1);
        int prioridad = 0;

        // Buscar cada campo candidato
        for (String candidato : CAMPOS_KEYSET_CANDIDATOS) {
            CampoKeyset campo = buscarCampoCandidato(
                    selectClause,
                    candidato,
                    tiposUsados,
                    prioridad
            );

            if (campo != null) {
                campos.add(campo);
                tiposUsados.add(campo.getTipoDato());
                prioridad++;
                log.debug("Campo keyset detectado: {} (tipo: {}, prioridad: {})",
                        campo.getNombreCampo(), campo.getTipoDato(), campo.getPrioridad());
            }
        }

        log.info("Detectados {} campos para keyset", campos.size());
        return campos;
    }

    /**
     * Busca un campo candidato específico en la cláusula SELECT.
     *
     * @param selectClause Cláusula SELECT del SQL
     * @param candidato Nombre del campo candidato a buscar
     * @param tiposUsados Tipos de dato ya utilizados (para evitar duplicados)
     * @param prioridad Prioridad del campo
     * @return CampoKeyset si se encontró, null en caso contrario
     */
    private CampoKeyset buscarCampoCandidato(
            String selectClause,
            String candidato,
            Set<TipoDatoKeyset> tiposUsados,
            int prioridad) {

        // Pattern para buscar el campo con su tabla (ej: i.fecha_infraccion)
        Pattern campoPattern = Pattern.compile(
                "\\b([a-zA-Z_]+\\." + Pattern.quote(candidato) + ")\\b",
                Pattern.CASE_INSENSITIVE
        );

        Matcher campoMatcher = campoPattern.matcher(selectClause);

        if (!campoMatcher.find()) {
            return null;
        }

        String nombreCompleto = campoMatcher.group(1);
        TipoDatoKeyset tipo = TipoDatoKeyset.detectarTipoDato(candidato);

        // Evitar duplicados de tipo (no dos fechas, no dos estados)
        if (estipoTemporal(tipo) && tiposUsados.stream().anyMatch(this::estipoTemporal)) {
            log.debug("Campo {} ignorado: ya existe un campo temporal", candidato);
            return null;
        }

        // Generar nombre de parámetro (ej: fecha_infraccion -> lastFechaInfraccion)
        String nombreParametro = "last" +
                candidato.substring(0, 1).toUpperCase() +
                candidato.substring(1).replaceAll("_", "");

        return new CampoKeyset(nombreCompleto, nombreParametro, tipo, prioridad);
    }

    /**
     * Verifica si un tipo de dato es temporal (DATE o TIMESTAMP).
     *
     * @param tipo Tipo de dato a verificar
     * @return true si es DATE o TIMESTAMP
     */
    private boolean estipoTemporal(TipoDatoKeyset tipo) {
        return tipo == TipoDatoKeyset.DATE || tipo == TipoDatoKeyset.TIMESTAMP;
    }

    // ==================== DETERMINACIÓN DE ESTRATEGIA ====================

    /**
     * Determina la estrategia de paginación basada en los campos disponibles.
     * MEJORADO: Ahora usa el alias correcto del ID.
     *
     * @param tieneIdInfracciones Si la query tiene ID de infracciones
     * @param aliasId Alias del campo ID (puede ser null)
     * @param camposDisponibles Campos detectados en el SELECT
     * @return Análisis con la estrategia determinada
     */
    private AnalisisPaginacion determinarEstrategiaPorCampos(
            boolean tieneIdInfracciones,
            String aliasId,
            List<CampoKeyset> camposDisponibles) {

        if (tieneIdInfracciones && camposDisponibles.size() >= 3) {
            // KEYSET CON ID: Necesitamos ID + al menos 3 campos más
            log.info("Estrategia: KEYSET_CON_ID (ID alias: '{}' + {} campos)",
                    aliasId, camposDisponibles.size());

            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEYSET_CON_ID,
                    true,
                    seleccionarCamposParaKeysetConId(camposDisponibles),
                    String.format("ID disponible (alias: '%s') + campos suficientes", aliasId)
            );
        }

        if (!tieneIdInfracciones && camposDisponibles.size() >= 3) {
            // KEYSET COMPUESTO: Sin ID pero con suficientes campos
            log.info("Estrategia: KEYSET_COMPUESTO ({} campos)", camposDisponibles.size());

            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEY_COMPUESTO,
                    false,
                    seleccionarCamposParaKeysetCompuesto(camposDisponibles),
                    "Sin ID pero campos suficientes para keyset compuesto"
            );
        }

        if (camposDisponibles.size() >= 1) {
            // FALLBACK: Usar LIMIT con algún campo para ordenar
            log.info("Estrategia: FALLBACK_LIMIT_ONLY ({} campos disponibles)",
                    camposDisponibles.size());

            return new AnalisisPaginacion(
                    EstrategiaPaginacion.FALLBACK_LIMIT_ONLY,
                    tieneIdInfracciones,
                    camposDisponibles,
                    String.format("Campos insuficientes para keyset completo (%d < 3)",
                            camposDisponibles.size())
            );
        }

        // OFFSET: Paginación tradicional (último recurso)
        log.warn("Estrategia: OFFSET (no se detectaron campos para keyset)");

        return new AnalisisPaginacion(
                EstrategiaPaginacion.OFFSET,
                false,
                Collections.emptyList(),
                "Campos insuficientes, usando paginación offset tradicional"
        );
    }

    // ==================== SELECCIÓN DE CAMPOS ====================

    /**
     * Selecciona los campos para KEYSET_CON_ID.
     * Toma los primeros 3 campos por prioridad.
     *
     * @param disponibles Lista de campos disponibles
     * @return Lista con los campos seleccionados
     */
    private List<CampoKeyset> seleccionarCamposParaKeysetConId(List<CampoKeyset> disponibles) {
        // Ordenar por prioridad y tomar los primeros 3
        disponibles.sort(Comparator.comparingInt(CampoKeyset::getPrioridad));
        int limite = Math.min(3, disponibles.size());

        log.debug("Seleccionados {} campos para KEYSET_CON_ID", limite);
        return new ArrayList<>(disponibles.subList(0, limite));
    }

    /**
     * Selecciona los campos para KEYSET_COMPUESTO.
     * Prioriza: TEXT -> DATE -> INTEGER -> BOOLEAN.
     *
     * @param disponibles Lista de campos disponibles
     * @return Lista con los campos seleccionados
     */
    private List<CampoKeyset> seleccionarCamposParaKeysetCompuesto(List<CampoKeyset> disponibles) {
        // Priorizar por tipo de dato, luego por prioridad
        disponibles.sort((a, b) -> {
            int prioridadA = getPrioridadTipo(a.getTipoDato());
            int prioridadB = getPrioridadTipo(b.getTipoDato());

            if (prioridadA != prioridadB) {
                return Integer.compare(prioridadA, prioridadB);
            }

            return Integer.compare(a.getPrioridad(), b.getPrioridad());
        });

        int limite = Math.min(4, disponibles.size());
        log.debug("Seleccionados {} campos para KEYSET_COMPUESTO", limite);

        return new ArrayList<>(disponibles.subList(0, limite));
    }

    // ==================== EXTRACCIÓN DE CAMPOS GROUP BY ====================

    /**
     * Extrae los campos utilizados en la cláusula GROUP BY.
     * Soporta tanto nombres de campos como posiciones numéricas.
     *
     * @param sql Query SQL a analizar
     * @return Lista de campos del GROUP BY
     */
    private List<CampoKeyset> extraerCamposGroupBy(String sql) {
        // Primero intentar GROUP BY con nombres de columnas
        List<CampoKeyset> campos = extraerGroupByPorNombres(sql);

        if (!campos.isEmpty()) {
            log.debug("Campos GROUP BY extraídos (nombres): {}", campos);
            return campos;
        }

        // Si no hay nombres, intentar GROUP BY numérico (1,2,3...)
        campos = extraerGroupByNumerico(sql);
        log.debug("Campos GROUP BY extraídos (numérico): {}", campos);

        return campos;
    }

    /**
     * Extrae campos GROUP BY cuando se usan nombres de columnas.
     *
     * @param sql Query SQL
     * @return Lista de campos extraídos
     */
    private List<CampoKeyset> extraerGroupByPorNombres(String sql) {
        List<CampoKeyset> campos = new ArrayList<>();
        Matcher matcher = GROUP_BY_NOMBRES_PATTERN.matcher(sql);

        if (!matcher.find()) {
            return campos;
        }

        String groupByClause = matcher.group(1).trim();
        String[] camposArray = groupByClause.split("\\s*,\\s*");

        for (int i = 0; i < Math.min(camposArray.length, 3); i++) {
            String campo = camposArray[i].trim();

            if (!campo.isEmpty()) {
                TipoDatoKeyset tipo = inferirTipoDato(campo);
                campos.add(new CampoKeyset(campo, campo, tipo, i));
            }
        }

        return campos;
    }

    /**
     * Extrae campos GROUP BY cuando se usan posiciones numéricas.
     *
     * @param sql Query SQL
     * @return Lista de campos extraídos
     */
    private List<CampoKeyset> extraerGroupByNumerico(String sql) {
        List<CampoKeyset> campos = new ArrayList<>();
        Matcher matcher = GROUP_BY_NUMERICO_PATTERN.matcher(sql);

        if (!matcher.find()) {
            return campos;
        }

        String groupByClause = matcher.group(1).trim();
        String[] numerosArray = groupByClause.split("\\s*,\\s*");

        // Extraer campos del SELECT
        List<String> camposSelect = extraerCamposDelSelect(sql);

        for (int i = 0; i < Math.min(numerosArray.length, 3); i++) {
            try {
                int posicion = Integer.parseInt(numerosArray[i].trim()) - 1; // 1-based to 0-based

                if (posicion >= 0 && posicion < camposSelect.size()) {
                    String campoCompleto = camposSelect.get(posicion);
                    String alias = extraerAlias(campoCompleto);

                    if (!alias.isEmpty()) {
                        TipoDatoKeyset tipo = inferirTipoDato(alias);
                        campos.add(new CampoKeyset(alias, campoCompleto, tipo, i));
                    }
                }
            } catch (NumberFormatException e) {
                log.warn("Posición GROUP BY inválida: {}", numerosArray[i]);
            }
        }

        return campos;
    }

    /**
     * Extrae los campos del SELECT para análisis de GROUP BY numérico.
     *
     * @param sql Query SQL
     * @return Lista de campos del SELECT
     */
    private List<String> extraerCamposDelSelect(String sql) {
        List<String> campos = new ArrayList<>();
        Matcher matcher = SELECT_PATTERN.matcher(sql);

        if (!matcher.find()) {
            return campos;
        }

        String selectClause = matcher.group(1).trim();

        // Dividir por comas (simple, no maneja funciones complejas perfectamente)
        String[] parts = selectClause.split(",");

        for (String part : parts) {
            campos.add(part.trim());
        }

        return campos;
    }

    /**
     * Extrae el alias de un campo SELECT.
     *
     * @param campo Campo completo del SELECT
     * @return Alias del campo
     */
    private String extraerAlias(String campo) {
        // Buscar "AS alias"
        Matcher matcher = ALIAS_PATTERN.matcher(campo);

        if (matcher.find()) {
            return matcher.group(1).trim();
        }

        // Si no tiene AS, tomar lo último después del punto
        if (campo.contains(".")) {
            String[] parts = campo.split("\\.");
            return parts[parts.length - 1].trim();
        }

        // Si no, devolver el campo completo (limpiando espacios)
        return campo.replaceAll("\\s+", "").trim();
    }

    /**
     * Infiere el tipo de dato de un campo basándose en su nombre.
     *
     * @param campo Nombre del campo
     * @return Tipo de dato inferido
     */
    private TipoDatoKeyset inferirTipoDato(String campo) {
        String lower = campo.toLowerCase();

        if (lower.contains("fecha") || lower.contains("date")) {
            return TipoDatoKeyset.DATE;
        }

        if (lower.contains("anio") || lower.contains("year")) {
            return TipoDatoKeyset.INTEGER;
        }

        return TipoDatoKeyset.TEXT;
    }
}