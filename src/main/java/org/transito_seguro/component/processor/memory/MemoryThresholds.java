package org.transito_seguro.component.processor.memory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.MemoryLevel;

/**
 * Configuración centralizada de umbrales de memoria.
 *
 * Responsabilidades:
 * - Definir umbrales críticos, altos y normales
 * - Validar configuración en startup
 * - Proporcionar información de umbrales
 */
@Slf4j
@Component
@Getter
public class MemoryThresholds {

    /**
     * Umbral CRÍTICO de memoria (85%).
     * Por encima de este valor, se sugiere GC y se reduce tamaño de lote.
     */
    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double criticalThreshold;

    /**
     * Umbral ALTO de memoria (70%).
     * Por encima de este valor, se pausa brevemente para dar tiempo al GC.
     */
    @Value("${app.batch.memory-high-threshold:0.70}")
    private double highThreshold;

    /**
     * Umbral NORMAL de memoria (50%).
     * Por debajo de este valor, la memoria se considera en estado normal.
     */
    @Value("${app.batch.memory-normal-threshold:0.50}")
    private double normalThreshold;

    /**
     * Tiempo de pausa en milisegundos cuando la memoria está alta.
     */
    @Value("${app.batch.memory-pause-duration-ms:50}")
    private int pauseDurationMs;

    /**
     * Tiempo de pausa después de sugerir GC.
     */
    @Value("${app.batch.memory-gc-pause-duration-ms:100}")
    private int gcPauseDurationMs;

    /**
     * Valida la configuración de umbrales al iniciar.
     * Se ejecuta automáticamente después de inyectar las propiedades.
     */
    @javax.annotation.PostConstruct
    public void validarConfiguracion() {
        log.info("🔧 Validando configuración de umbrales de memoria...");

        // Validar rangos (0.0 - 1.0)
        validarRango("critical", criticalThreshold);
        validarRango("high", highThreshold);
        validarRango("normal", normalThreshold);

        // Validar orden lógico
        if (criticalThreshold <= highThreshold) {
            log.warn("⚠️ CONFIGURACIÓN INVÁLIDA: critical ({}) debe ser > high ({})",
                    criticalThreshold, highThreshold);
            throw new IllegalStateException(
                    "Umbral crítico debe ser mayor que umbral alto");
        }

        if (highThreshold <= normalThreshold) {
            log.warn("⚠️ CONFIGURACIÓN INVÁLIDA: high ({}) debe ser > normal ({})",
                    highThreshold, normalThreshold);
            throw new IllegalStateException(
                    "Umbral alto debe ser mayor que umbral normal");
        }

        // Validar tiempos de pausa
        if (pauseDurationMs < 0 || pauseDurationMs > 10000) {
            log.warn("⚠️ pauseDurationMs fuera de rango: {} (debe ser 0-10000)", pauseDurationMs);
            throw new IllegalStateException("Duración de pausa inválida");
        }

        if (gcPauseDurationMs < 0 || gcPauseDurationMs > 10000) {
            log.warn("⚠️ gcPauseDurationMs fuera de rango: {} (debe ser 0-10000)", gcPauseDurationMs);
            throw new IllegalStateException("Duración de pausa GC inválida");
        }

        log.info("✅ Configuración de umbrales validada:");
        log.info("   🔴 Crítico: {:.0f}%", criticalThreshold * 100);
        log.info("   🟠 Alto: {:.0f}%", highThreshold * 100);
        log.info("   🟢 Normal: {:.0f}%", normalThreshold * 100);
        log.info("   ⏸️  Pausa: {}ms | Pausa GC: {}ms", pauseDurationMs, gcPauseDurationMs);
    }

    /**
     * Valida que un umbral esté en el rango válido (0.0 - 1.0).
     */
    private void validarRango(String nombre, double valor) {
        if (valor < 0.0 || valor > 1.0) {
            log.error("❌ Umbral {} fuera de rango: {} (debe ser 0.0 - 1.0)", nombre, valor);
            throw new IllegalStateException(
                    String.format("Umbral %s fuera de rango: %f", nombre, valor));
        }
    }

    /**
     * Determina el nivel de memoria actual según umbrales.
     *
     * @param porcentajeUsado Porcentaje de memoria usada (0.0 - 1.0)
     * @return Nivel de memoria
     */
    public MemoryLevel determinarNivel(double porcentajeUsado) {
        if (porcentajeUsado >= criticalThreshold) {
            return MemoryLevel.CRITICAL;
        } else if (porcentajeUsado >= highThreshold) {
            return MemoryLevel.HIGH;
        } else if (porcentajeUsado >= normalThreshold) {
            return MemoryLevel.NORMAL;
        } else {
            return MemoryLevel.LOW;
        }
    }

    /**
     * Obtiene el factor de reducción de lote según nivel de memoria.
     *
     * Usado por MemoryMonitor para ajustar tamaños dinámicamente.
     *
     * @param nivel Nivel de memoria actual
     * @return Factor de reducción (1.0 = sin reducción, 0.25 = reducir a 25%)
     */
    public double getFactorReduccionLote(MemoryLevel nivel) {
        switch (nivel) {
            case CRITICAL:
                return 0.25;  // Reducir a 25%
            case HIGH:
                return 0.50;  // Reducir a 50%
            case NORMAL:
            case LOW:
            default:
                return 1.0;   // Sin reducción
        }
    }


    /**
     * Obtiene información detallada de la configuración actual.
     *
     * @return String con información formateada
     */
    public String obtenerInformacion() {
        return String.format(
                "Umbrales de Memoria:%n" +
                        "  🔴 Crítico: %.0f%%%n" +
                        "  🟠 Alto: %.0f%%%n" +
                        "  🟢 Normal: %.0f%%%n" +
                        "  ⏸️  Pausas: %dms (normal) / %dms (GC)",
                criticalThreshold * 100,
                highThreshold * 100,
                normalThreshold * 100,
                pauseDurationMs,
                gcPauseDurationMs
        );
    }
}