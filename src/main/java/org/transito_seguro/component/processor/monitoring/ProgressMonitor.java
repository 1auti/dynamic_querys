package org.transito_seguro.component.processor.monitoring;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitor de progreso para procesamiento paralelo.
 *
 * Responsabilidades:
 * - Reportar progreso cada N segundos
 * - Mostrar estado por provincia
 * - Calcular porcentajes de completitud
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProgressMonitor {

    private final MemoryMonitor memoryMonitor;

    /**
     * Inicia monitoreo en background para procesamiento paralelo.
     *
     * @param totalProvincias Total de provincias a procesar
     * @param provinciasCompletadas Contador de provincias completadas
     * @param provinciasEnProceso Contador de provincias en proceso
     * @param estadoPorProvincia Mapa con estado de cada provincia
     * @return Future para cancelar el monitoreo
     */
    public ScheduledFuture<?> iniciarMonitoreoParalelo(
            int totalProvincias,
            AtomicInteger provinciasCompletadas,
            AtomicInteger provinciasEnProceso,
            Map<String, String> estadoPorProvincia) {

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();

        return monitor.scheduleAtFixedRate(() -> {
            reportarProgresoParalelo(
                    totalProvincias,
                    provinciasCompletadas.get(),
                    provinciasEnProceso.get(),
                    estadoPorProvincia
            );
        }, 2, 3, TimeUnit.SECONDS); // Reporta cada 3 segundos
    }

    /**
     * Reporta progreso actual del procesamiento paralelo.
     */
    private void reportarProgresoParalelo(
            int total,
            int completadas,
            int enProceso,
            Map<String, String> estados) {

        if (completadas >= total) {
            return; // Ya terminó
        }

        double progreso = (double) completadas / total * 100;

        log.info("═══════════════════════════════════════════════════════════");
        log.info("📊 PROGRESO PARALELO: {}/{} provincias ({:.1f}%)",
                completadas, total, progreso);
        log.info("   En proceso: {} | Memoria: {:.1f}%",
                enProceso, memoryMonitor.obtenerPorcentajeMemoriaUsada());

        // Mostrar estado detallado
        estados.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry ->
                        log.info("   {} - {}", entry.getKey(), entry.getValue())
                );

        log.info("═══════════════════════════════════════════════════════════");
    }
}