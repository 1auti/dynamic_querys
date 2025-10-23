package org.transito_seguro.component.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.analyzer.*;
import org.transito_seguro.model.CampoAnalizado;
import org.transito_seguro.model.FiltroMetadata;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;

import java.util.*;

import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;

/**
 * Componente orquestador para el análisis completo de queries SQL.
 * Coordinar el análisis delegando a componentes especializados.
 */
@Slf4j
@Component
public class QueryAnalyzer {

    @Autowired
    private SqlParser sqlParser;

    @Autowired
    private FieldClassifier fieldClassifier;

    @Autowired
    private ConsolidationAnalyzer consolidationAnalyzer;

    @Autowired
    private FilterDetector filterDetector;

    /**
     * Analiza una query SQL para determinar la estrategia óptima de consolidación.
     *
     * Proceso de análisis (delegado):
     * 1. Extrae y parsea la cláusula SELECT → {@link SqlParser}
     * 2. Clasifica campos por tipo → {@link FieldClassifier}
     * 3. Detecta presencia de GROUP BY → {@link SqlParser}
     * 4. Estima cantidad de registros → {@link ConsolidationAnalyzer}
     * 5. Determina tipo de consolidación → {@link ConsolidationAnalyzer}
     *
     * @param query Query SQL a analizar
     * @return Análisis completo con estrategia recomendada
     */
    public AnalisisConsolidacion analizarParaConsolidacion(String query) {
        log.debug("=== INICIANDO ANÁLISIS DE QUERY ===");
        log.debug("Query: {}", query);

        try {
            // PASO 1: Extraer cláusula SELECT (delegado a SqlParser)
            String selectClause = sqlParser.extraerSelectClause(query);
            if (selectClause == null) {
                log.warn("No se pudo extraer SELECT de la query");
                return crearAnalisisVacio();
            }

            // PASO 2: Parsear campos individuales
            List<CampoAnalizado> campos = parsearCamposSelect(selectClause);
            log.debug("Campos parseados: {}", campos.size());

            // PASO 3: Clasificar campos por tipo y propósito (delegado a FieldClassifier)
            AnalisisConsolidacion analisisBase = clasificarCamposParaConsolidacion(campos, query);

            // PASO 4: Verificar si es consolidable
            if (!analisisBase.isEsConsolidable()) {
                log.warn("Query NO consolidable - No cumple requisitos mínimos");
                return analisisBase;
            }

            // PASO 5: Determinar tipo de consolidación (delegado a ConsolidationAnalyzer)
            return completarAnalisisConsolidacion(analisisBase, query);

        } catch (Exception e) {
            log.error("Error crítico analizando query: {}", e.getMessage(), e);
            return crearAnalisisVacio();
        }
    }

    /**
     * Detecta filtros disponibles en la query.
     *
     * DELEGADO completamente a {@link FilterDetector}.
     *
     * @param query Query SQL a analizar
     * @return Map con filtros detectados y su metadata
     */
    public Map<String, FiltroMetadata> detectarFiltrosDisponibles(String query) {
        return filterDetector.detectarFiltrosDisponibles(query);
    }

    /**
     * Determina si una query es consolidable.
     *
     * @param sql Query SQL
     * @return true si tiene GROUP BY
     */
    public boolean esQueryConsolidable(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        return sqlParser.tieneGroupBy(sql);
    }

    // ========== MÉTODOS PRIVADOS DE COORDINACIÓN ==========

    /**
     * Parsea la cláusula SELECT dividiéndola en campos individuales.
     * Delega el parsing real a SqlParser y la clasificación a FieldClassifier.
     */
    private List<CampoAnalizado> parsearCamposSelect(String selectClause) {
        List<CampoAnalizado> campos = new ArrayList<>();

        // Dividir campos (delegado a SqlParser)
        List<String> camposRaw = sqlParser.dividirCamposInteligente(selectClause);

        // Analizar cada campo (delegado a FieldClassifier)
        for (String campoRaw : camposRaw) {
            CampoAnalizado campo = fieldClassifier.analizarCampoIndividual(campoRaw.trim());
            if (campo != null) {
                campos.add(campo);
                log.trace("Campo parseado: {} → {} (tipo: {})",
                        campo.expresionOriginal, campo.nombreFinal, campo.tipo);
            }
        }

        log.debug("Parseados {} campos del SELECT", campos.size());
        return campos;
    }

    /**
     * Clasifica los campos parseados según su rol en la consolidación.
     *
     * Coordina la información ya clasificada por FieldClassifier.
     */
    private AnalisisConsolidacion clasificarCamposParaConsolidacion(
            List<CampoAnalizado> campos,
            String query) {

        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();
        List<String> camposTiempo = new ArrayList<>();
        List<String> camposUbicacion = new ArrayList<>();
        Map<String, org.transito_seguro.enums.TipoCampo> tipoPorCampo = new HashMap<>();

        // Clasificar campos según tipo (ya determinado por FieldClassifier)
        for (CampoAnalizado campo : campos) {
            tipoPorCampo.put(campo.nombreFinal, campo.tipo);

            switch (campo.tipo) {
                case UBICACION:
                    camposUbicacion.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case CATEGORIZACION:
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case TIEMPO:
                    camposTiempo.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case NUMERICO_SUMA:
                case NUMERICO_COUNT:
                    camposNumericos.add(campo.nombreFinal);
                    break;

                case CALCULADO:
                    if (campo.nombreFinal.contains("enviado") ||
                            campo.nombreFinal.contains("hacia") ||
                            campo.nombreFinal.contains("categoria")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                case IDENTIFICADOR:
                case DETALLE:
                    if (campo.nombreFinal.equals("serie_equipo") ||
                            campo.nombreFinal.equals("lugar")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                default:
                    log.warn("Tipo de campo no manejado: {} ({})", campo.nombreFinal, campo.tipo);
                    break;
            }
        }

        // Una query es consolidable si tiene campos numéricos Y agrupación
        boolean esConsolidable = !camposNumericos.isEmpty() && !camposAgrupacion.isEmpty();

        // Si no hay ubicación explícita, agregar "provincia" por defecto
        if (camposUbicacion.isEmpty() && esConsolidable) {
            camposUbicacion.add("provincia");
            if (!camposAgrupacion.contains("provincia")) {
                camposAgrupacion.add("provincia");
            }
        }

        log.info("Clasificación completada - Consolidable: {}, Agrupación: {}, Numéricos: {}",
                esConsolidable, camposAgrupacion.size(), camposNumericos.size());

        return new AnalisisConsolidacion(
                camposAgrupacion,
                camposNumericos,
                camposTiempo,
                camposUbicacion,
                tipoPorCampo,
                esConsolidable,
                null,  // Tipo se determina después
                null,  // Estimación se calcula después
                0.0,   // Confianza se calcula después
                ""     // Explicación se genera después
        );
    }

    /**
     * Completa el análisis determinando el tipo de consolidación.
     *
     * DELEGA la decisión final a ConsolidationAnalyzer.
     */
    private AnalisisConsolidacion completarAnalisisConsolidacion(
            AnalisisConsolidacion analisisBase,
            String query) {

        // Verificar GROUP BY (delegado a SqlParser)
        boolean tieneGroupBy = sqlParser.tieneGroupBy(query);
        log.debug("Tiene GROUP BY: {}", tieneGroupBy);

        if (tieneGroupBy) {
            // Query agregada: delegar a ConsolidationAnalyzer
            List<String> camposGroupBy = sqlParser.extraerCamposGroupBy(query);
            return consolidationAnalyzer.analizarQueryAgregada(analisisBase, camposGroupBy);
        } else {
            // Query cruda: delegar a ConsolidationAnalyzer
            return consolidationAnalyzer.analizarQueryCruda(analisisBase, query);
        }
    }

    // ========== GETTERS PARA UMBRALES (delegados) ==========

    public static int getUmbralAgregadaStreaming() {
        return ConsolidationAnalyzer.getUmbralAgregadaStreaming();
    }

    public static int getUmbralAgregadaMemoria() {
        return ConsolidationAnalyzer.getUmbralAgregadaMemoria();
    }

    public static int getUmbralCrudoForzarAgregacion() {
        return ConsolidationAnalyzer.getUmbralCrudoForzarAgregacion();
    }

    public static int getUmbralCrudoStreaming() {
        return ConsolidationAnalyzer.getUmbralCrudoStreaming();
    }
}
