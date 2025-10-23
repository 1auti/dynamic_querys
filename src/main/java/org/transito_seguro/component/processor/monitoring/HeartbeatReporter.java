package org.transito_seguro.component.processor.monitoring;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reporta heartbeat periódico durante procesamiento largo.
 *
 * Responsabilidades:
 * - Logging periódico de progreso
 * - Evitar spam de logs (máximo cada 30 segundos)
 * - Mostrar métricas de tiempo y memoria
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HeartbeatReporter {

    private final MemoryMonitor memoryMonitor;

    private static final long HEARTBEAT_INTERVAL_MS = 30000; // 30 segundos

    private final AtomicLong ultimoHeartbeat = new AtomicLong(0);
    private final AtomicLong tiempoInicio = new AtomicLong(0);
    private final AtomicInteger totalRegistrosGlobales = new AtomicInteger(0);

    /**
     * Inicializa el heartbeat.
     */
    public void inicializar() {
        long ahora = System.currentTimeMillis();
        tiempoInicio.set(ahora);
        ultimoHeartbeat.set(ahora);
        totalRegistrosGlobales.set(0);
    }

    /**
     * Reporta heartbeat si ha pasado suficiente tiempo.
     *
     * @param totalRepositorios Total de repositorios/provincias
     */
    public void reportar(int totalRepositorios) {
        long ahora = System.currentTimeMillis();
        long ultimo = ultimoHeartbeat.get();

        if (ahora - ultimo > HEARTBEAT_INTERVAL_MS) {
            ultimoHeartbeat.set(ahora);

            long duracion = ahora - tiempoInicio.get();

            log.info("💓 Heartbeat - {}s | {} registros | Memoria: {:.1f}%",
                    duracion / 1000,
                    totalRegistrosGlobales.get(),
                    memoryMonitor.obtenerPorcentajeMemoriaUsada());
        }
    }

    /**
     * Actualiza contador de registros procesados.
     */
    public void actualizarRegistros(int cantidad) {
        totalRegistrosGlobales.addAndGet(cantidad);
    }

    /**
     * Obtiene total de registros procesados.
     */
    public int getTotalRegistros() {
        return totalRegistrosGlobales.get();
    }
}