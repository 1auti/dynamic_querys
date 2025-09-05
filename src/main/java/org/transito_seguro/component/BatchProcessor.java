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

    @Value("${app.batch.chunk-size:250}")
    private int chunkSize;

    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double memoryCriticalThreshold;

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

            while (hayMasDatos && !esMemoriaCritica()) {
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
                    lote.forEach(registro -> registro.put("provincia_origen", provincia));

                    log.debug("Procesando lote: provincia={}, offset={}, tamaño={}",
                            provincia, offset, lote.size());

                    // NUEVO: Procesar el lote en chunks pequeños
                    procesarLoteEnChunks(lote, procesadorLote);

                    totalProcesados += lote.size();
                    offset += batchSize;

                    // NUEVO: Liberar explícitamente la referencia del lote
                    lote.clear();
                    lote = null;

                    // Verificar si obtuvimos menos registros de los esperados
                    if (lote.size() < batchSize) {
                        hayMasDatos = false;
                    }

                    // MEJORADO: Pausa inteligente sin GC forzado
                    if (offset % (batchSize * 5) == 0) {
                        pausaInteligente();
                    }

                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, offset {}: {}",
                            provincia, offset, e.getMessage(), e);
                    break;
                }
            }

            if (esMemoriaCritica()) {
                log.warn("Procesamiento pausado por memoria crítica en provincia: {}", provincia);
                esperarLiberacionMemoria();
            }

            log.info("Completada provincia {}: registros procesados en esta iteración", provincia);
        }

        log.info("Procesamiento en lotes completado. Total procesados: {}", totalProcesados);
    }

    /**
     * Divide un lote en chunks más pequeños y los procesa uno por uno
     */
    private void procesarLoteEnChunks(List<Map<String, Object>> lote,
                                      Consumer<List<Map<String, Object>>> procesadorLote) {

        int currentChunkSize = Math.min(chunkSize, lote.size());

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());

            // Crear sublista (vista, no copia completa)
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            // Procesar el chunk
            procesadorLote.accept(chunk);

            // Liberar referencia explícitamente
            chunk = null;

            // Verificar memoria cada 10 chunks
            if (i > 0 && (i / currentChunkSize) % 10 == 0 && esMemoriaAlta()) {
                pausaMicro();
            }
        }
    }

    /**
     * Verifica si la memoria está en nivel crítico
     */
    private boolean esMemoriaCritica() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > memoryCriticalThreshold;
    }

    /**
     * Verifica si la memoria está alta (pero no crítica)
     */
    private boolean esMemoriaAlta() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        double porcentajeUso = (double) memoriaUsada / runtime.totalMemory();
        return porcentajeUso > 0.70;
    }

    /**
     * Pausa inteligente que permite GC natural sin forzarlo
     */
    private void pausaInteligente() {
        try {
            // Pausa muy corta para permitir que el GC natural actúe
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Pausa microscópica para chunks
     */
    private void pausaMicro() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Espera activa cuando la memoria está crítica
     */
    private void esperarLiberacionMemoria() {
        int intentos = 0;
        while (esMemoriaCritica() && intentos < 10) {
            try {
                log.warn("Memoria crítica detectada, esperando liberación... (intento {})", intentos + 1);
                Thread.sleep(1000); // Espera 1 segundo
                intentos++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
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

        // Ajuste más agresivo basado en memoria
        if (porcentajeLibre < 0.20) { // Menos del 20% libre
            batchSize = Math.max(100, batchSize / 8);
            log.warn("Memoria muy baja detectada ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSize);
        } else if (porcentajeLibre < 0.30) { // Menos del 30% libre
            batchSize = Math.max(200, batchSize / 4);
            log.warn("Memoria baja detectada ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSize);
        } else if (porcentajeLibre < 0.50) { // Menos del 50% libre
            batchSize = Math.max(400, batchSize / 2);
            log.info("Memoria media detectada ({}%), reduciendo lote a {}",
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
        // Solo sugerir GC si realmente es necesario
        if (esMemoriaCritica()) {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            // Solo una pausa pequeña para permitir GC natural
            pausaInteligente();
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

    /**
     * Obtiene estadísticas extendidas incluyendo chunks
     */
    public Map<String, Object> obtenerEstadisticasMemoriaExtendidas() {
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

        // Nuevas estadísticas
        stats.put("chunk_size_configurado", chunkSize);
        stats.put("memoria_critica_threshold", memoryCriticalThreshold);
        stats.put("es_memoria_critica", esMemoriaCritica());
        stats.put("es_memoria_alta", esMemoriaAlta());

        return stats;
    }

}