package org.transito_seguro.component.analyzer.pagination;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.analyzer.query.SqlParser;
import org.transito_seguro.enums.TipoDatoKeyset;
import org.transito_seguro.model.CampoKeyset;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Componente especializado en detectar campos disponibles para keyset pagination.
 *
 * Responsabilidades:
 * - Detectar ID de infracciones y su alias
 * - Detectar campos candidatos en el SELECT
 * - Clasificar tipos de datos (DATE, INTEGER, TEXT, etc.)
 * - Extraer campos del GROUP BY para queries consolidadas
 */
@Slf4j
@Component
public class KeysetFieldDetector {

    @Autowired
    private SqlParser sqlParser;

    // ========== CONSTANTES ==========

    /**
     * Campos candidatos para keyset pagination (ordenados por prioridad).
     */
    private static final List<String> CAMPOS_CANDIDATOS = Arrays.asList(
            "serie_equipo",
            "id_tipo_infra",
            "fecha_infraccion",
            "id_estado",
            "id_punto_control",
            "packedfile"
    );

    /**
     * Pattern para detectar i.id o infracciones.id con alias.
     */
    private static final Pattern ID_INFRACCIONES_PATTERN = Pattern.compile(
            "SELECT\\s+.*?\\b(i\\.id|infracciones\\.id)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?.*?FROM",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Pattern para detectar SELECT.
     */
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            "SELECT\\s+(.*?)\\s+FROM",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    // ========== DETECCIÓN DE ID INFRACCIONES ==========

    /**
     * Detecta si la query tiene ID de infracciones y retorna su alias.
     *
     * Ejemplos:
     * - "SELECT i.id FROM..." → "id"
     * - "SELECT i.id AS id_infraccion FROM..." → "id_infraccion"
     * - "SELECT infracciones.id AS pk FROM..." → "pk"
     *
     * @param sql Query SQL a analizar
     * @return Alias del campo ID o null si no existe
     */
    public String detectarAliasIdInfracciones(String sql) {
        Matcher matcher = ID_INFRACCIONES_PATTERN.matcher(sql);

        if (matcher.find()) {
            String aliasExplicito = matcher.group(2);

            if (aliasExplicito != null && !aliasExplicito.isEmpty()) {
                log.debug("ID infracciones con alias explícito: {}", aliasExplicito);
                return aliasExplicito;
            }

            log.debug("ID infracciones sin alias, usando 'id' por defecto");
            return "id";
        }

        log.debug("No se detectó ID de infracciones en el SELECT");
        return null;
    }

    // ========== DETECCIÓN DE CAMPOS DISPONIBLES ==========

    /**
     * Detecta campos disponibles en el SELECT para usar en keyset.
     *
     * @param sql Query SQL a analizar
     * @return Lista de campos detectados con metadata
     */
    public List<CampoKeyset> detectarCamposDisponibles(String sql) {
        List<CampoKeyset> campos = new ArrayList<>();
        Set<TipoDatoKeyset> tiposUsados = new HashSet<>();

        // Extraer cláusula SELECT usando SqlParser
        String selectClause = sqlParser.extraerSelectClause(sql);
        if (selectClause == null) {
            log.warn("No se pudo extraer cláusula SELECT");
            return campos;
        }

        int prioridad = 0;

        // Buscar cada campo candidato
        for (String candidato : CAMPOS_CANDIDATOS) {
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
                log.debug("Campo keyset detectado: {} (tipo: {})",
                        campo.getNombreCampo(), campo.getTipoDato());
            }
        }

        log.info("Detectados {} campos para keyset", campos.size());
        return campos;
    }

    /**
     * Busca un campo candidato específico en la cláusula SELECT.
     */
    private CampoKeyset buscarCampoCandidato(
            String selectClause,
            String candidato,
            Set<TipoDatoKeyset> tiposUsados,
            int prioridad) {

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

        // Evitar duplicados de tipo temporal
        if (esTipoTemporal(tipo) && tiposUsados.stream().anyMatch(this::esTipoTemporal)) {
            log.debug("Campo {} ignorado: ya existe un campo temporal", candidato);
            return null;
        }

        String nombreParametro = generarNombreParametro(candidato);

        return new CampoKeyset(nombreCompleto, nombreParametro, tipo, prioridad);
    }

    /**
     * Genera nombre de parámetro estilo camelCase.
     *
     * Ejemplo: "fecha_infraccion" → "lastFechaInfraccion"
     */
    private String generarNombreParametro(String candidato) {
        return "last" +
                candidato.substring(0, 1).toUpperCase() +
                candidato.substring(1).replaceAll("_", "");
    }

    // ========== EXTRACCIÓN DE CAMPOS GROUP BY ==========

    /**
     * Extrae campos utilizados en GROUP BY para queries consolidadas.
     *
     * @param sql Query SQL a analizar
     * @return Lista de campos del GROUP BY
     */
    public List<CampoKeyset> extraerCamposGroupBy(String sql) {
        // Extraer campos GROUP BY usando SqlParser
        List<String> camposGroupBy = sqlParser.extraerCamposGroupBy(sql);

        if (camposGroupBy.isEmpty()) {
            log.debug("No se encontraron campos GROUP BY");
            return Collections.emptyList();
        }

        // Convertir a CampoKeyset con metadata
        List<CampoKeyset> campos = new ArrayList<>();

        for (int i = 0; i < Math.min(camposGroupBy.size(), 3); i++) {
            String campo = camposGroupBy.get(i);
            TipoDatoKeyset tipo = inferirTipoDato(campo);

            campos.add(new CampoKeyset(campo, campo, tipo, i));
        }

        log.debug("Campos GROUP BY convertidos: {}", campos);
        return campos;
    }

    // ========== MÉTODOS AUXILIARES ==========

    /**
     * Verifica si un tipo de dato es temporal.
     */
    private boolean esTipoTemporal(TipoDatoKeyset tipo) {
        return tipo == TipoDatoKeyset.DATE || tipo == TipoDatoKeyset.TIMESTAMP;
    }

    /**
     * Infiere el tipo de dato basándose en el nombre del campo.
     */
    private TipoDatoKeyset inferirTipoDato(String campo) {
        String lower = campo.toLowerCase();

        if (lower.contains("fecha") || lower.contains("date")) {
            return TipoDatoKeyset.DATE;
        }

        if (lower.contains("anio") || lower.contains("year") ||
                lower.contains("id_") || lower.contains("estado")) {
            return TipoDatoKeyset.INTEGER;
        }

        if (lower.contains("serie") || lower.contains("packed")) {
            return TipoDatoKeyset.TEXT;
        }

        return TipoDatoKeyset.TEXT; // Default
    }
}