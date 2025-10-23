package org.transito_seguro.component.processor.monitoring;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Recolector de métricas del procesamiento.
 *
 * Responsabilidades:
 * - Registrar cambios de estrategia por OOM
 * - Contadores por provincia
 * - Reportar métricas finales
 */
@Slf4j
@Component
public class MetricsCollector {

    @Getter
    private final AtomicInteger cambiosEstrategiaPorOOM = new AtomicInteger(0);

    @Getter
    private final Map<String, Integer> contadoresPorProvincia = new ConcurrentHashMap<>();

    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);

    /**
     * Inicializa métricas.
     */
    public void inicializar() {
        cambiosEstrategiaPorOOM.set(0);
        contadoresPorProvincia.clear();
        totalRegistrosGlobales.set(0);
    }

    /**
     * Registra cambio de estrategia por OOM.
     */
    public void registrarCambioEstrategiaPorOOM() {
        cambiosEstrategiaPorOOM.incrementAndGet();
        log.warn("⚠️ Cambio de estrategia AGREGACION→CRUDO por OOM (total: {})",
                cambiosEstrategiaPorOOM.get());
    }

    /**
     * Actualiza contador de una provincia.
     */
    public void actualizarContadorProvincia(String provincia, int cantidad) {
        contadoresPorProvincia.merge(provincia, cantidad, Integer::sum);
        totalRegistrosGlobales.addAndGet(cantidad);
    }

    /**
     * Obtiene total de registros procesados.
     */
    public int getTotalRegistros() {
        return totalRegistrosGlobales.get();
    }

    /**
     * Reporta métricas finales del ciclo de vida del BatchProcessor.
     */
    public void reportarMetricasFinales() {
        log.info("═══════════════════════════════════════════════════════════");
        log.info("📊 MÉTRICAS FINALES DE PROCESAMIENTO");
        log.info("═══════════════════════════════════════════════════════════");

        // Cambios de estrategia por OOM
        int cambios = cambiosEstrategiaPorOOM.get();
        if (cambios > 0) {
            log.warn("⚠️ Cambios de estrategia AGREGACION→CRUDO: {}", cambios);
            log.info("💡 Recomendación: Revisar queries con GROUP BY para mejorar estimaciones");
            log.info("   - Ejecutar ANALYZE en las tablas");
            log.info("   - Revisar cardinalidad de campos en GROUP BY");
            log.info("   - Considerar pre-agregar datos en vistas materializadas");
        } else {
            log.info("✅ Sin cambios de estrategia por OOM detectados");
        }

        // Total de registros procesados
        int totalRegistros = totalRegistrosGlobales.get();
        if (totalRegistros > 0) {
            log.info("📈 Total de registros procesados: {:,}", totalRegistros);
        }

        // Provincias procesadas
        if (!contadoresPorProvincia.isEmpty()) {
            log.info("🗺️ Provincias procesadas: {}", contadoresPorProvincia.size());

            // Top 5 provincias con más registros
            var top5 = contadoresPorProvincia.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(5)
                    .collect(Collectors.toList());

            if (!top5.isEmpty()) {
                log.info("📊 Top 5 provincias por volumen:");
                for (int i = 0; i < top5.size(); i++) {
                    Map.Entry<String, Integer> entry = top5.get(i);
                    log.info("   {}. {}: {:,} registros",
                            i + 1, entry.getKey(), entry.getValue());
                }
            }
        }

        log.info("═══════════════════════════════════════════════════════════");
    }
}