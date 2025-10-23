package org.transito_seguro.component.processor.strategy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.execution.QueryExecutor;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.component.processor.monitoring.HeartbeatReporter;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.List;

/**
 * Estrategia de procesamiento SECUENCIAL.
 *
 * Procesa una provincia a la vez, en orden.
 *
 * Uso recomendado:
 * - > 200K registros por provincia
 * - Datasets masivos
 * - Memoria limitada
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SequentialStrategy implements ProcessingStrategy {

    private final QueryExecutor queryExecutor;
    private final MemoryMonitor memoryMonitor;
    private final HeartbeatReporter heartbeatReporter;

    @Override
    public void ejecutar(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("🐌 Ejecutando modo SECUENCIAL - {} provincias una por una", repositories.size());

        for (int i = 0; i < repositories.size(); i++) {
            InfraccionesRepositoryImpl repo = repositories.get(i);
            String provincia = repo.getProvincia();

            try {
                // Heartbeat cada provincia
                heartbeatReporter.reportar(repositories.size());

                log.info("▶️  Procesando provincia {} ({}/{})",
                        provincia, i + 1, repositories.size());

                // Ejecutar provincia (delegado a QueryExecutor)
                queryExecutor.ejecutarProvincia(
                        repo,
                        filtros,
                        nombreQuery,
                        provincia,
                        contexto,
                        queryStorage
                );

                // Procesar resultados acumulados de esta provincia
                contexto.procesarTodosResultados();

                log.info("✅ {} completada ({}/{})", provincia, i + 1, repositories.size());

                // Pausa si memoria alta (antes de la siguiente provincia)
                if (i < repositories.size() - 1 && memoryMonitor.esMemoriaAlta()) {
                    log.warn("⚠️ Memoria alta ({:.1f}%) - Pausando antes de siguiente provincia",
                            memoryMonitor.obtenerPorcentajeMemoriaUsada());
                    memoryMonitor.pausarSiNecesario();
                }

            } catch (Exception e) {
                log.error("❌ Error procesando {}: {}", provincia, e.getMessage(), e);
                // Continuar con siguiente provincia
            }
        }

        log.info("═══════════════════════════════════════════════════════════");
        log.info("✅ PROCESAMIENTO SECUENCIAL COMPLETADO");
        log.info("   Total: {} provincias procesadas", repositories.size());
        log.info("═══════════════════════════════════════════════════════════");
    }

    @Override
    public String getNombre() {
        return "SECUENCIAL";
    }
}