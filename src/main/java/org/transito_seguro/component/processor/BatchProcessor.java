package org.transito_seguro.component.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.estimation.DatasetEstimator;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.component.processor.monitoring.HeartbeatReporter;
import org.transito_seguro.component.processor.monitoring.MetricsCollector;
import org.transito_seguro.component.processor.strategy.*;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.service.QueryRegistryService;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Orquestador principal de procesamiento por lotes.
 *
 * RESPONSABILIDAD ÚNICA: Coordinar el procesamiento delegando a componentes especializados.
 *
 * Delega a:
 * - {@link DatasetEstimator}: Estimación de volumen
 * - {@link ProcessingStrategy}: Estrategias de procesamiento (Paralelo/Secuencial/Híbrido)
 * - {@link MemoryMonitor}: Gestión de memoria
 * - {@link HeartbeatReporter}: Monitoreo de progreso
 * - {@link MetricsCollector}: Recolección de métricas
 *
 * @author Transito Seguro Team
 * @version 3.0 - Refactorizado siguiendo Single Responsibility Principle
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchProcessor {

    // ========== COMPONENTES DELEGADOS ==========
    private final DatasetEstimator datasetEstimator;
    private final MemoryMonitor memoryMonitor;
    private final HeartbeatReporter heartbeatReporter;
    private final MetricsCollector metricsCollector;
    private final QueryRegistryService queryRegistryService;

    // Estrategias de procesamiento
    private final ParallelStrategy parallelStrategy;
    private final SequentialStrategy sequentialStrategy;
    private final HybridStrategy hybridStrategy;

    // ========== UMBRALES CONFIGURABLES ==========
    @Value("${app.batch.parallel-threshold-per-province:50000}")
    private int parallelThresholdPerProvince;

    @Value("${app.batch.parallel-threshold-total:300000}")
    private int parallelThresholdTotal;

    @Value("${app.batch.massive-threshold-per-province:200000}")
    private int massiveThresholdPerProvince;

    // ========== MÉTODO PRINCIPAL ==========

    /**
     * Procesa datos en lotes delegando a la estrategia apropiada.
     *
     * @param repositories Lista de repositorios (uno por provincia)
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @param procesarLotes Consumer para procesar resultados
     */
    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesarLotes) {

        // Inicializar monitoreo
        heartbeatReporter.inicializar();
        metricsCollector.inicializar();

        logInicio(repositories.size(), nombreQuery);

        try {
            // PASO 1: Estimar volumen de datos (delegado)
            EstimacionDataset estimacion = datasetEstimator.estimarDataset(
                    repositories,
                    filtros,
                    nombreQuery
            );
            logEstimacion(estimacion);

            // PASO 2: Decidir estrategia según estimación
            EstrategiaProcessing estrategia = decidirEstrategia(estimacion);
            log.info("🎯 Estrategia seleccionada: {}", estrategia);

            // PASO 3: Obtener metadata de la query
            QueryStorage queryStorage = queryRegistryService.buscarQuery(nombreQuery)
                    .orElse(null);

            if (queryStorage == null) {
                log.warn("⚠️ Query no encontrada: {}", nombreQuery);
                return;
            }

            // PASO 4: Crear contexto de procesamiento
            ContextoProcesamiento contexto = new ContextoProcesamiento(procesarLotes, null);

            // PASO 5: Ejecutar estrategia (delegado)
            ejecutarEstrategia(estrategia, repositories, filtros, nombreQuery, contexto, queryStorage);

        } finally {
            imprimirResumenFinal();
            metricsCollector.reportarMetricasFinales();
        }
    }

    // ========== MÉTODOS PRIVADOS ==========

    /**
     * Ejecuta la estrategia seleccionada.
     */
    private void ejecutarEstrategia(
            EstrategiaProcessing estrategia,
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        ProcessingStrategy strategy;

        switch (estrategia) {
            case PARALELO:
                strategy = parallelStrategy;
                break;
            case HIBRIDO:
                strategy = hybridStrategy;
                break;
            case SECUENCIAL:
                strategy = sequentialStrategy;
                break;
            default:
                log.warn("⚠️ Estrategia desconocida: {}, usando SECUENCIAL", estrategia);
                strategy = sequentialStrategy;
        }

        log.info("🚀 Ejecutando estrategia: {}", strategy.getNombre());
        strategy.ejecutar(repositories, filtros, nombreQuery, contexto, queryStorage);
    }

    /**
     * Decide la estrategia de procesamiento según la estimación.
     */
    private EstrategiaProcessing decidirEstrategia(EstimacionDataset estimacion) {
        if (estimacion.getPromedioPorProvincia() < parallelThresholdPerProvince &&
                estimacion.getTotalEstimado() < parallelThresholdTotal) {
            return EstrategiaProcessing.PARALELO;
        }

        if (estimacion.getMaximoPorProvincia() > massiveThresholdPerProvince) {
            return EstrategiaProcessing.SECUENCIAL;
        }

        return EstrategiaProcessing.HIBRIDO;
    }

    // ========== LOGGING ==========

    private void logInicio(int numProvincias, String nombreQuery) {
        log.info("═══════════════════════════════════════════════════════════");
        log.info("🚀 Inicio procesamiento - Provincias: {} | Query: {} | Memoria: {:.1f}%",
                numProvincias, nombreQuery, memoryMonitor.obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");
    }

    private void logEstimacion(EstimacionDataset estimacion) {
        log.info("📊 Estimación - Total: {} | Promedio: {} reg/prov | Máximo: {}",
                estimacion.getTotalEstimado(),
                (int) estimacion.getPromedioPorProvincia(),
                estimacion.getMaximoPorProvincia());
    }

    private void imprimirResumenFinal() {
        log.info("═══════════════════════════════════════════════════════════");
        log.info("✅ PROCESAMIENTO COMPLETADO");
        log.info("   Total registros: {} | Memoria: {:.1f}%",
                heartbeatReporter.getTotalRegistros(),
                memoryMonitor.obtenerPorcentajeMemoriaUsada());
        log.info("═══════════════════════════════════════════════════════════");
    }

    // ========== SHUTDOWN ==========

    @PreDestroy
    public void shutdown() {
        log.info("🛑 Cerrando BatchProcessor...");
        metricsCollector.reportarMetricasFinales();
        log.info("✅ BatchProcessor cerrado correctamente");
    }
}