package org.transito_seguro.component.processor.strategy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.execution.QueryExecutor;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Estrategia de procesamiento HÍBRIDO.
 *
 * Procesa provincias en grupos pequeños (ej: 6 por vez),
 * esperando que cada grupo termine antes de iniciar el siguiente.
 *
 * Uso recomendado:
 * - 50K - 200K registros por provincia
 * - Dataset total moderado
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HybridStrategy implements ProcessingStrategy {

    private final QueryExecutor queryExecutor;
    private final MemoryMonitor memoryMonitor;
    private final ExecutorService parallelExecutor;

    @Value("${app.batch.max-parallel-provinces:6}")
    private int maxParallelProvinces;

    @Override
    public void ejecutar(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("⚡ Ejecutando modo HÍBRIDO - Grupos de {} provincias", maxParallelProvinces);

        int totalProvincias = repositories.size();
        int gruposProcesados = 0;
        int totalGrupos = (int) Math.ceil((double) totalProvincias / maxParallelProvinces);

        // Procesar en grupos
        for (int i = 0; i < totalProvincias; i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, totalProvincias);
            List<InfraccionesRepositoryImpl> grupo = repositories.subList(i, endIndex);

            gruposProcesados++;

            log.info("═══════════════════════════════════════════════════════════");
            log.info("📦 GRUPO {}/{}: Procesando {} provincias ({}-{})",
                    gruposProcesados, totalGrupos,
                    grupo.size(), i + 1, endIndex);
            log.info("═══════════════════════════════════════════════════════════");

            // Procesar grupo en paralelo
            procesarGrupo(grupo, filtros, nombreQuery, contexto, queryStorage);

            // Procesar resultados acumulados del grupo
            contexto.procesarTodosResultados();

            log.info("✅ GRUPO {}/{} completado | Memoria: {:.1f}%",
                    gruposProcesados, totalGrupos,
                    memoryMonitor.obtenerPorcentajeMemoriaUsada());

            // Pausa entre grupos si memoria alta
            if (endIndex < totalProvincias && memoryMonitor.esMemoriaAlta()) {
                log.warn("⚠️ Memoria alta - Pausando antes del siguiente grupo");
                memoryMonitor.pausarSiNecesario();
                memoryMonitor.sugerirGcSiNecesario();
            }
        }

        log.info("═══════════════════════════════════════════════════════════");
        log.info("✅ PROCESAMIENTO HÍBRIDO COMPLETADO");
        log.info("   Total: {} grupos procesados ({} provincias)",
                gruposProcesados, totalProvincias);
        log.info("═══════════════════════════════════════════════════════════");
    }

    /**
     * Procesa un grupo de provincias en paralelo.
     */
    private void procesarGrupo(
            List<InfraccionesRepositoryImpl> grupo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        // Crear futures para cada provincia del grupo
        List<CompletableFuture<Void>> futures = grupo.stream()
                .map(repo -> CompletableFuture.runAsync(() -> {
                    String provincia = repo.getProvincia();

                    try {
                        log.info("▶️  Iniciando {}", provincia);

                        queryExecutor.ejecutarProvincia(
                                repo,
                                filtros,
                                nombreQuery,
                                provincia,
                                contexto,
                                queryStorage
                        );

                        log.info("✅ {} completada", provincia);

                    } catch (Exception e) {
                        log.error("❌ Error en {}: {}", provincia, e.getMessage(), e);
                    }
                }, parallelExecutor))
                .collect(Collectors.toList());

        // Esperar a que todas las provincias del grupo terminen
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public String getNombre() {
        return "HÍBRIDO";
    }
}
