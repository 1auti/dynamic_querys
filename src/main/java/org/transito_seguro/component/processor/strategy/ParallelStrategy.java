package org.transito_seguro.component.processor.strategy;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.execution.QueryExecutor;
import org.transito_seguro.component.processor.monitoring.ProgressMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Estrategia de procesamiento PARALELO.
 *
 * Procesa todas las provincias simultáneamente usando un thread pool.
 * Uso recomendado:
 * - < 50K registros por provincia
 * - < 300K registros totales
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ParallelStrategy implements ProcessingStrategy {

    private final QueryExecutor queryExecutor;
    private final ProgressMonitor progressMonitor;
    private final ExecutorService parallelExecutor;

    @Override
    public void ejecutar(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("🚀 Ejecutando modo PARALELO - {} provincias simultáneas", repositories.size());

        // Contadores para seguimiento
        AtomicInteger provinciasCompletadas = new AtomicInteger(0);
        AtomicInteger provinciasEnProceso = new AtomicInteger(0);
        Map<String, String> estadoPorProvincia = new ConcurrentHashMap<>();

        // Iniciar monitoreo en background
        ScheduledFuture<?> tareaMonitoreo = progressMonitor.iniciarMonitoreoParalelo(
                repositories.size(),
                provinciasCompletadas,
                provinciasEnProceso,
                estadoPorProvincia
        );

        try {
            // Crear futures para cada provincia
            List<CompletableFuture<Void>> futures = repositories.stream()
                    .map(repo -> CompletableFuture.runAsync(() ->
                                    procesarProvinciaParalela(
                                            repo,
                                            filtros,
                                            nombreQuery,
                                            contexto,
                                            queryStorage,
                                            provinciasCompletadas,
                                            provinciasEnProceso,
                                            estadoPorProvincia,
                                            repositories.size()
                                    ),
                            parallelExecutor
                    ))
                    .collect(Collectors.toList());

            // Esperar a que todas terminen
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } finally {
            // Detener monitoreo
            tareaMonitoreo.cancel(false);

            // Reporte final
            log.info("═══════════════════════════════════════════════════════════");
            log.info("✅ PROCESAMIENTO PARALELO COMPLETADO");
            log.info("   Total: {}/{} provincias",
                    provinciasCompletadas.get(), repositories.size());
            log.info("═══════════════════════════════════════════════════════════");

            // Procesar resultados acumulados
            contexto.procesarTodosResultados();
        }
    }

    /**
     * Procesa una provincia individual en modo paralelo.
     */
    private void procesarProvinciaParalela(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage,
            AtomicInteger provinciasCompletadas,
            AtomicInteger provinciasEnProceso,
            Map<String, String> estadoPorProvincia,
            int totalProvincias) {

        String provincia = repo.getProvincia();

        try {
            // Marcar inicio
            provinciasEnProceso.incrementAndGet();
            estadoPorProvincia.put(provincia, "🔄 PROCESANDO");
            log.info("▶️  Iniciando procesamiento de {}", provincia);

            // Ejecutar provincia (delegado a QueryExecutor)
            queryExecutor.ejecutarProvincia(repo, filtros, nombreQuery, provincia, contexto, queryStorage);

            // Marcar completado
            estadoPorProvincia.put(provincia, "✅ COMPLETADO");
            int completadas = provinciasCompletadas.incrementAndGet();
            provinciasEnProceso.decrementAndGet();

            log.info("✅ {} completada ({}/{})", provincia, completadas, totalProvincias);

        } catch (Exception e) {
            estadoPorProvincia.put(provincia, "❌ ERROR: " + e.getMessage());
            provinciasEnProceso.decrementAndGet();
            log.error("❌ Error en {}: {}", provincia, e.getMessage(), e);
        }
    }

    @Override
    public String getNombre() {
        return "PARALELO";
    }
}