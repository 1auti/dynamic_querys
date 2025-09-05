package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.batch.size:1000}")
    private int defaultBatchSize;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryPerBatchMB;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    /**
     * Procesa datos en lotes de manera eficiente
     */
    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("Iniciando procesamiento en lotes para {} repositorios", repositories.size());

        int batchSize = calcularTamanoLoteOptimo(filtros);
        int totalProcesados = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();
            log.debug("Procesando provincia: {}", provincia);

            int offset = 0;
            boolean hayMasDatos = true;

            while (hayMasDatos) {
                try {
                    // Crear filtros específicos para este lote
                    ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset);

                    // Ejecutar consulta para este lote
                    List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                    if (lote.isEmpty()) {
                        hayMasDatos = false;
                        break;
                    }

                    // Agregar información de provincia a cada registro
                    lote.forEach(registro ->
                            registro.put("provincia_origen", provincia)
                    );

                    log.debug("Procesando lote: provincia={}, offset={}, tamaño={}",
                            provincia, offset, lote.size());

                    // Procesar el lote (convertir formato, escribir archivo, etc.)
                    procesadorLote.accept(lote);

                    totalProcesados += lote.size();
                    offset += batchSize;

                    // Verificar si obtuvimos menos registros de los esperados
                    if (lote.size() < batchSize) {
                        hayMasDatos = false;
                    }

                    // Sugerir al garbage collector que limpie
                    if (offset % (batchSize * 5) == 0) {
                        System.gc();
                        Thread.sleep(50); // Pequeña pausa para permitir GC
                    }

                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, offset {}: {}",
                            provincia, offset, e.getMessage(), e);
                    break;
                }
            }

            log.info("Completada provincia {}: {} registros procesados", provincia, totalProcesados);
        }

        log.info("Procesamiento en lotes completado. Total procesados: {}", totalProcesados);
    }

    /**
     * Calcula el tamaño óptimo de lote basado en filtros y memoria disponible
     */
    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
        int batchSize = defaultBatchSize;


        // Considerar memoria disponible
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaTotal = runtime.totalMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaTotal;

        if (porcentajeLibre < 0.3) { // Menos del 30% de memoria libre
            batchSize = Math.max(100, batchSize / 4);
            log.warn("Memoria baja detectada ({}%), reduciendo tamaño de lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSize);
        }

        return Math.min(batchSize, filtros.getLimiteMaximo() != null ?
                filtros.getLimiteMaximo() : Integer.MAX_VALUE);
    }

    /**
     * Crea filtros específicos para un lote
     */
    private ParametrosFiltrosDTO crearFiltrosParaLote(ParametrosFiltrosDTO filtrosOriginales,
                                                      int batchSize, int offset) {
        return filtrosOriginales.toBuilder()
                .pagina(offset / batchSize)
                .limite(batchSize)
                .build();
    }

    /**
     * Estimación del uso de memoria por registro (aproximado)
     */
    private long estimarMemoriaPorRegistro(Map<String, Object> registro) {
        // Estimación aproximada: cada campo como 50 bytes promedio
        return registro.size() * 50L;
    }

    /**
     * Verifica si hay suficiente memoria para procesar un lote
     */
    public boolean hayMemoriaSuficiente(int tamanoLote) {
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaEstimada = tamanoLote * 1024L; // 1KB por registro estimado

        return memoriaLibre > (memoriaEstimada * 2); // Factor de seguridad x2
    }

    /**
     * Fuerza limpieza de memoria
     */
    public void limpiarMemoria() {
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Obtiene estadísticas de memoria actual
     */
    public Map<String, Object> obtenerEstadisticasMemoria() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaTotal = runtime.totalMemory();
        long memoriaLibre = runtime.freeMemory();
        long memoriaUsada = memoriaTotal - memoriaLibre;
        long memoriaMaxima = runtime.maxMemory();

        Map<String, Object> stats = new HashMap<>();
        stats.put("memoria_total_mb", memoriaTotal / (1024 * 1024));
        stats.put("memoria_libre_mb", memoriaLibre / (1024 * 1024));
        stats.put("memoria_usada_mb", memoriaUsada / (1024 * 1024));
        stats.put("memoria_maxima_mb", memoriaMaxima / (1024 * 1024));
        stats.put("porcentaje_uso", (double) memoriaUsada / memoriaTotal * 100);

        return stats;
    }

}