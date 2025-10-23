package org.transito_seguro.component.analyzer.query;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;

import java.util.*;

/**
 * Componente especializado en determinar la estrategia óptima de consolidación.
 *
 * Responsabilidades:
 * - Calcular estimación de registros (queries agregadas)
 * - Determinar tipo de consolidación (AGREGACION, STREAMING, CRUDO)
 * - Aplicar umbrales de decisión
 * - Generar recomendaciones técnicas
 */
@Slf4j
@Component
public class ConsolidationAnalyzer {

    @Autowired
    private FieldClassifier fieldClassifier;

    // ========== UMBRALES ==========
    private static final int UMBRAL_AGREGADA_STREAMING = 100_000;
    private static final int UMBRAL_AGREGADA_MEMORIA = 50_000;
    private static final int UMBRAL_CRUDO_FORZAR_AGREGACION = 100_000;
    private static final int UMBRAL_CRUDO_STREAMING = 10_000;

    /**
     * Analiza una query agregada (con GROUP BY).
     */
    public AnalisisConsolidacion analizarQueryAgregada(
            AnalisisConsolidacion analisisBase,
            List<String> camposGroupBy) {

        log.debug("--- Analizando QUERY AGREGADA (con GROUP BY) ---");

        EstimacionRegistros estimacion = estimarRegistrosAgregados(camposGroupBy, analisisBase);

        TipoConsolidacion tipo;
        String recomendacion;
        String estrategiaTecnica;

        if (estimacion.registrosEstimados < UMBRAL_AGREGADA_MEMORIA) {
            tipo = TipoConsolidacion.AGREGACION;
            recomendacion = "Volumen bajo - Carga completa en memoria";
            estrategiaTecnica = "HashMap con consolidación en memoria";

        } else if (estimacion.registrosEstimados < UMBRAL_AGREGADA_STREAMING) {
            tipo = TipoConsolidacion.AGREGACION_STREMING;
            recomendacion = "Volumen medio - Streaming recomendado";
            estrategiaTecnica = "Procesar por chunks de 5k registros";

        } else {
            tipo = TipoConsolidacion.AGREGACION_ALTO_VOLUMEN;
            recomendacion = "Volumen ALTO - Streaming optimizado obligatorio";
            estrategiaTecnica = "Chunks de 10k + ConcurrentHashMap";
        }

        String explicacion = String.format(
                "%s | %s | Técnica: %s | Confianza: %.0f%%",
                estimacion.explicacion,
                recomendacion,
                estrategiaTecnica,
                estimacion.confianza * 100
        );

        log.info("📊 RESULTADO: Tipo={}, Estimación={:,} registros",
                tipo, estimacion.registrosEstimados);

        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,
                tipo,
                estimacion.registrosEstimados,
                estimacion.confianza,
                explicacion
        );
    }

    /**
     * Analiza una query cruda (sin GROUP BY).
     */
    public AnalisisConsolidacion analizarQueryCruda(AnalisisConsolidacion analisisBase, String query) {
        log.warn("--- Analizando QUERY CRUDA (sin GROUP BY) ---");

        String explicacion = "Query SIN GROUP BY - Requiere COUNT(*) dinámico para determinar estrategia";

        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,
                TipoConsolidacion.CRUDO,
                null,
                0.0,
                explicacion
        );
    }

    /**
     * Estima registros multiplicando cardinalidades de campos GROUP BY.
     */
    private EstimacionRegistros estimarRegistrosAgregados(
            List<String> camposGroupBy,
            AnalisisConsolidacion analisis) {

        if (camposGroupBy.isEmpty()) {
            return new EstimacionRegistros(1, 1.0, "Sin GROUP BY → 1 registro agregado");
        }

        long producto = 1;
        int camposConocidos = 0;
        int camposDesconocidos = 0;
        List<String> detalleCalculo = new ArrayList<>();

        for (String campo : camposGroupBy) {
            Integer cardinalidad = fieldClassifier.obtenerCardinalidadConocida(campo);

            if (cardinalidad != null) {
                producto *= cardinalidad;
                camposConocidos++;
                detalleCalculo.add(campo + "=" + cardinalidad);
            } else {
                TipoCampo tipo = analisis.getTipoPorCampo().get(campo);
                int cardinalidadEstimada = fieldClassifier.estimarCardinalidadPorTipo(campo, tipo);
                producto *= cardinalidadEstimada;
                camposDesconocidos++;
                detalleCalculo.add(campo + "≈" + cardinalidadEstimada);
            }
        }

        double confianza = camposDesconocidos == 0 ? 1.0 :
                (double) camposConocidos / camposGroupBy.size();

        int estimacionFinal = (int) Math.min(producto, 10_000_000);

        String formula = String.join(" × ", detalleCalculo);
        String explicacion = String.format(
                "GROUP BY [%s]: %s = %,d registros (%d conocidos, %d estimados)",
                String.join(", ", camposGroupBy),
                formula,
                estimacionFinal,
                camposConocidos,
                camposDesconocidos
        );

        return new EstimacionRegistros(estimacionFinal, confianza, explicacion);
    }

    // Clase interna
    public static class EstimacionRegistros {
        public int registrosEstimados;
        public double confianza;
        public String explicacion;

        public EstimacionRegistros(int registros, double confianza, String explicacion) {
            this.registrosEstimados = registros;
            this.confianza = confianza;
            this.explicacion = explicacion;
        }
    }

    // Getters de umbrales
    public static int getUmbralAgregadaStreaming() { return UMBRAL_AGREGADA_STREAMING; }
    public static int getUmbralAgregadaMemoria() { return UMBRAL_AGREGADA_MEMORIA; }
    public static int getUmbralCrudoForzarAgregacion() { return UMBRAL_CRUDO_FORZAR_AGREGACION; }
    public static int getUmbralCrudoStreaming() { return UMBRAL_CRUDO_STREAMING; }

}

