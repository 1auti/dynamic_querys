package org.transito_seguro.component.processor.memory;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.MemoryLevel;
import org.transito_seguro.model.MemoryStats;

/**
 * Monitor de memoria para prevenir OutOfMemoryError.
 *
 * ACTUALIZADO: Usa MemoryThresholds para configuración centralizada.
 *
 * Responsabilidades:
 * - Monitorear uso de memoria en tiempo real
 * - Aplicar umbrales configurables (delegado a MemoryThresholds)
 * - Sugerir pausas cuando sea necesario
 *
 * @author Transito Seguro Team
 * @version 3.1 - Refactorizado para usar MemoryThresholds
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MemoryMonitor {

    private final MemoryThresholds thresholds;

    @Getter
    private final Runtime runtime = Runtime.getRuntime();

    /**
     * Obtiene el porcentaje de memoria usada.
     *
     * @return Porcentaje entre 0.0 y 100.0
     */
    public double obtenerPorcentajeMemoriaUsada() {
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        long memoriaMaxima = runtime.maxMemory();
        return (double) memoriaUsada / memoriaMaxima * 100;
    }

    /**
     * Obtiene el porcentaje como decimal (0.0 - 1.0).
     *
     * @return Porcentaje decimal
     */
    public double obtenerPorcentajeMemoriaUsadaDecimal() {
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        long memoriaMaxima = runtime.maxMemory();
        return (double) memoriaUsada / memoriaMaxima;
    }

    /**
     * Verifica si la memoria está en nivel CRÍTICO.
     *
     * @return true si está en nivel crítico
     */
    public boolean esMemoriaCritica() {
        double porcentaje = obtenerPorcentajeMemoriaUsadaDecimal();
        boolean esCritica = porcentaje > thresholds.getCriticalThreshold();

        if (esCritica) {
            log.warn("⚠️ MEMORIA CRÍTICA: {:.1f}% (umbral: {:.1f}%)",
                    porcentaje * 100, thresholds.getCriticalThreshold() * 100);
        }

        return esCritica;
    }

    /**
     * Verifica si la memoria está en nivel ALTO.
     *
     * @return true si está en nivel alto
     */
    public boolean esMemoriaAlta() {
        double porcentaje = obtenerPorcentajeMemoriaUsadaDecimal();
        return porcentaje > thresholds.getHighThreshold();
    }

    /**
     * Verifica si la memoria está en nivel NORMAL.
     *
     * @return true si está en nivel normal
     */
    public boolean esMemoriaNormal() {
        double porcentaje = obtenerPorcentajeMemoriaUsadaDecimal();
        return porcentaje <= thresholds.getHighThreshold()
                && porcentaje >= thresholds.getNormalThreshold();
    }

    /**
     * Obtiene el nivel actual de memoria.
     *
     * @return Nivel de memoria (LOW, NORMAL, HIGH, CRITICAL)
     */
    public MemoryLevel obtenerNivelMemoria() {
        double porcentaje = obtenerPorcentajeMemoriaUsadaDecimal();
        return thresholds.determinarNivel(porcentaje);
    }

    /**
     * Pausa el thread si la memoria está alta.
     * Útil para dar tiempo al GC.
     */
    public void pausarSiNecesario() {
        if (esMemoriaAlta()) {
            try {
                int duracion = thresholds.getPauseDurationMs();
                log.debug("⏸️ Pausando {}ms para liberar memoria ({:.1f}%)",
                        duracion, obtenerPorcentajeMemoriaUsada());
                Thread.sleep(duracion);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("⚠️ Pausa de memoria interrumpida");
            }
        }
    }

    /**
     * Sugiere GC si la memoria está crítica.
     */
    public void sugerirGcSiNecesario() {
        if (esMemoriaCritica()) {
            double porcentajeAntes = obtenerPorcentajeMemoriaUsada();

            log.info("🗑️ Sugiriendo Garbage Collection (memoria: {:.1f}%)", porcentajeAntes);
            System.gc();

            // Esperar a que el GC actúe
            try {
                Thread.sleep(thresholds.getGcPauseDurationMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            double porcentajeDespues = obtenerPorcentajeMemoriaUsada();
            double liberado = porcentajeAntes - porcentajeDespues;

            if (liberado > 0) {
                log.info("✅ Memoria después de GC: {:.1f}% (liberado: {:.1f}%)",
                        porcentajeDespues, liberado);
            } else {
                log.warn("⚠️ GC no liberó memoria significativa: {:.1f}% → {:.1f}%",
                        porcentajeAntes, porcentajeDespues);
            }
        }
    }

    /**
     * Calcula tamaño de lote óptimo según memoria disponible.
     *
     * Usa los factores de reducción definidos en MemoryThresholds.
     *
     * @param batchSizeBase Tamaño base deseado
     * @return Tamaño ajustado según memoria
     */
    public int calcularTamanoLoteOptimo(int batchSizeBase) {
        MemoryLevel nivel = obtenerNivelMemoria();
        double factor = thresholds.getFactorReduccionLote(nivel);

        int tamanoAjustado = (int) (batchSizeBase * factor);

        // Asegurar un mínimo de 1000 registros
        tamanoAjustado = Math.max(1000, tamanoAjustado);

        // Limitar a máximo 10K
        tamanoAjustado = Math.min(tamanoAjustado, 10000);

        if (factor < 1.0) {
            log.info("📏 Tamaño de lote ajustado por memoria {}: {} → {} (factor: {:.0f}%)",
                    nivel, batchSizeBase, tamanoAjustado, factor * 100);
        }

        return tamanoAjustado;
    }

    /**
     * Obtiene información detallada de memoria para logging.
     *
     * @return Objeto con estadísticas de memoria
     */
    public MemoryStats obtenerEstadisticas() {
        long memoriaMaxima = runtime.maxMemory();
        long memoriaTotal = runtime.totalMemory();
        long memoriaLibre = runtime.freeMemory();
        long memoriaUsada = memoriaTotal - memoriaLibre;
        double porcentajeUsado = (double) memoriaUsada / memoriaMaxima * 100;
        MemoryLevel nivel = obtenerNivelMemoria();

        return new MemoryStats(
                memoriaMaxima,
                memoriaTotal,
                memoriaUsada,
                memoriaLibre,
                porcentajeUsado,
                nivel
        );
    }


}