package org.transito_seguro.component.analyzer.pagination;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoDatoKeyset;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.*;

import static org.transito_seguro.model.CampoKeyset.getPrioridadTipo;

/**
 * Componente especializado en seleccionar la estrategia óptima de paginación.
 *
 * Responsabilidades:
 * - Determinar estrategia según campos disponibles
 * - Seleccionar campos óptimos para cada estrategia
 * - Aplicar reglas de priorización
 *
 * @author Transito Seguro Team
 * @version 3.0 - Refactorizado
 */
@Slf4j
@Component
public class PaginationStrategySelector {

    /**
     * Determina la estrategia de paginación según campos disponibles.
     *
     * @param tieneIdInfracciones Si la query tiene ID de infracciones
     * @param aliasId Alias del campo ID (puede ser null)
     * @param camposDisponibles Campos detectados en el SELECT
     * @return Análisis con la estrategia determinada
     */
    public AnalisisPaginacion determinarEstrategia(
            boolean tieneIdInfracciones,
            String aliasId,
            List<CampoKeyset> camposDisponibles) {

        // CASO 1: KEYSET CON ID (óptimo)
        if (tieneIdInfracciones && camposDisponibles.size() >= 3) {
            return crearAnalisisKeysetConId(aliasId, camposDisponibles);
        }

        // CASO 2: KEYSET COMPUESTO (sin ID pero suficientes campos)
        if (!tieneIdInfracciones && camposDisponibles.size() >= 3) {
            return crearAnalisisKeysetCompuesto(camposDisponibles);
        }

        // CASO 3: FALLBACK LIMIT (pocos campos)
        if (camposDisponibles.size() >= 1) {
            return crearAnalisisFallbackLimit(tieneIdInfracciones, camposDisponibles);
        }

        // CASO 4: OFFSET (último recurso)
        return crearAnalisisOffset();
    }

    /**
     * Determina estrategia para query consolidada (con GROUP BY).
     *
     * @param camposGroupBy Campos del GROUP BY
     * @return Análisis de paginación
     */
    public AnalisisPaginacion determinarEstrategiaConsolidada(List<CampoKeyset> camposGroupBy) {
        if (camposGroupBy.size() >= 2) {
            log.info("Query consolidada con keyset: {} campos GROUP BY", camposGroupBy.size());

            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEY_COMPUESTO,
                    false,
                    camposGroupBy,
                    "Keyset basado en campos GROUP BY"
            );
        }

        log.info("Query consolidada sin keyset viable (solo {} campos)", camposGroupBy.size());

        return new AnalisisPaginacion(
                EstrategiaPaginacion.SIN_PAGINACION,
                false,
                Collections.emptyList(),
                "GROUP BY insuficiente para keyset (mínimo 2 campos requeridos)"
        );
    }

    // ========== CREACIÓN DE ANÁLISIS POR ESTRATEGIA ==========

    /**
     * Crea análisis para estrategia KEYSET_CON_ID.
     */
    private AnalisisPaginacion crearAnalisisKeysetConId(
            String aliasId,
            List<CampoKeyset> camposDisponibles) {

        List<CampoKeyset> camposSeleccionados = seleccionarCamposParaKeysetConId(camposDisponibles);

        log.info("Estrategia: KEYSET_CON_ID (ID alias: '{}' + {} campos)",
                aliasId, camposSeleccionados.size());

        return new AnalisisPaginacion(
                EstrategiaPaginacion.KEYSET_CON_ID,
                true,
                camposSeleccionados,
                String.format("ID disponible (alias: '%s') + campos suficientes", aliasId)
        );
    }

    /**
     * Crea análisis para estrategia KEY_COMPUESTO.
     */
    private AnalisisPaginacion crearAnalisisKeysetCompuesto(List<CampoKeyset> camposDisponibles) {
        List<CampoKeyset> camposSeleccionados = seleccionarCamposParaKeysetCompuesto(camposDisponibles);

        log.info("Estrategia: KEY_COMPUESTO ({} campos)", camposSeleccionados.size());

        return new AnalisisPaginacion(
                EstrategiaPaginacion.KEY_COMPUESTO,
                false,
                camposSeleccionados,
                "Sin ID pero campos suficientes para keyset compuesto"
        );
    }

    /**
     * Crea análisis para estrategia FALLBACK_LIMIT_ONLY.
     */
    private AnalisisPaginacion crearAnalisisFallbackLimit(
            boolean tieneIdInfracciones,
            List<CampoKeyset> camposDisponibles) {

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

    /**
     * Crea análisis para estrategia OFFSET (último recurso).
     */
    private AnalisisPaginacion crearAnalisisOffset() {
        log.warn("Estrategia: OFFSET (no se detectaron campos para keyset)");

        return new AnalisisPaginacion(
                EstrategiaPaginacion.OFFSET,
                false,
                Collections.emptyList(),
                "Campos insuficientes, usando paginación offset tradicional"
        );
    }

    // ========== SELECCIÓN DE CAMPOS ==========

    /**
     * Selecciona los campos óptimos para KEYSET_CON_ID.
     * Toma los primeros 3 campos por prioridad.
     */
    private List<CampoKeyset> seleccionarCamposParaKeysetConId(List<CampoKeyset> disponibles) {
        disponibles.sort(Comparator.comparingInt(CampoKeyset::getPrioridad));
        int limite = Math.min(3, disponibles.size());

        log.debug("Seleccionados {} campos para KEYSET_CON_ID", limite);
        return new ArrayList<>(disponibles.subList(0, limite));
    }

    /**
     * Selecciona los campos óptimos para KEYSET_COMPUESTO.
     * Prioriza: TEXT -> DATE -> INTEGER -> BOOLEAN.
     */
    private List<CampoKeyset> seleccionarCamposParaKeysetCompuesto(List<CampoKeyset> disponibles) {
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
}