package org.transito_seguro.component;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaProcessing;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Component
public class BatchProcessor {

    @Value("${app.batch.size:5000}")
    private int defaultBatchSize;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryPerBatchMB;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${app.batch.chunk-size:250}")
    private int chunkSize;

    @Value("${app.batch.memory-critical-threshold:0.85}")
    private double memoryCriticalThreshold;

    // Properties nuevas
    @Value("${app.batch.parallel-threshold-per-province:50000}")
    private int parallelThresholdPerProvince;

    @Value("${app.batch.parallel-threshold-total:300000}")
    private int parallelThresholdTotal;

    @Value("${app.batch.massive-threshold-per-province:200000}")
    private int massiveThresholdPerProvince;

    @Value("${app.batch.max-parallel-provinces:6}")
    private int maxParallelProvinces;

    // Sirve para crear tareas en paralelo sin tener que crear y destruir hilos manualmente
    private final Executor parallelExecutor = Executors.newFixedThreadPool(6);
    private Consumer<List<Map<String, Object>>> procesadorLoteGlobal;
    private BufferedWriter archivoSalida;
    private Queue<Map<String, Object>> colaResultados = new ConcurrentLinkedQueue<>();

    /**
     * Método para configurar el procesador antes de ejecutar
     */
    public void configurarProcesadorInmediato(Consumer<List<Map<String, Object>>> procesador) {
        this.procesadorLoteGlobal = procesador;
    }



    // ============= CLASE AUXILLAR ===============================

    @AllArgsConstructor
    private static class EstimacionDataset{
        final int totalEstimado;
        final double promedioPorProvincia;
        final int maximoPorProvincia;
        final List<Integer> muestrasPorProvicia;
    }


    /**
     * Procesamiento datos en lotes de forma
     * */

    public void procesarEnLotes(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String,Object>>> procesarLotes
    ){

        log.info("Iniciando procesamiento de lotes para {} repositorios",repositories.size());

        // 1. Estimacion
        EstimacionDataset estimacionDataset = estimarDataset(repositories,filtros,nombreQuery);
        log.info("Estimacion {} total, {} por provincia promedio",
                estimacionDataset.totalEstimado,estimacionDataset.promedioPorProvincia);

        // 2. Decision Estrategia
        EstrategiaProcessing estrategia = decidirEstrategia(estimacionDataset);
        log.info("Estrategia selecionada {}",estrategia);

        // 3. Ejecutamos la Estrategia

        switch (estrategia){
            case PARALELO:
                procesarParaleloCompletoConStreaming(repositories,filtros,nombreQuery,procesarLotes);
                break;
            case HIBRIDO:
                procesarHibridoControlado(repositories,filtros,nombreQuery,procesarLotes,estimacionDataset);
                break;
            case SECUENCIAL:
                procesarEnLotesFormaSecuencial(repositories,filtros,nombreQuery,procesarLotes);
                break;
        }
    }


    /**
     * Estimacion del tamanio del dataset
     * */

    private EstimacionDataset estimarDataset(List<InfraccionesRepositoryImpl> repositories,
                                             ParametrosFiltrosDTO filtros,
                                             String nombreQuery){

        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(50)
                .offset(0)
                .build();

        List<Integer> muestras = repositories.parallelStream()
                .map(repo -> {
                    try {
                        List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);

                        // Estimación basada en tamaño de muestra
                        if (muestra.size() >= 50) {
                            return 10000; // Extrapolación conservadora
                        } else if (muestra.size() >= 20) {
                            return muestra.size() * 500;
                        } else if (muestra.size() > 0) {
                            return muestra.size() * 200;
                        }
                        return 0;

                    } catch (Exception e) {
                        log.warn("Error estimando para provincia {}: {}", repo.getProvincia(), e.getMessage());
                        return 1000; // Estimación conservadora
                    }
                })
                .collect(Collectors.toList());

        int totalEstimado = muestras.stream().mapToInt(Integer::intValue).sum();
        int promedioPorProvincia = totalEstimado / repositories.size();
        int maximoPorProvincia = muestras.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedioPorProvincia, maximoPorProvincia, muestras);

    }

    /**
     * Estrategia
     * */

    private EstrategiaProcessing decidirEstrategia(EstimacionDataset estimacion) {

        // CRITERIO 1: Dataset pequeño-mediano → PARALELO COMPLETO
        if (estimacion.promedioPorProvincia < parallelThresholdPerProvince &&
                estimacion.totalEstimado < parallelThresholdTotal) {
            return EstrategiaProcessing.PARALELO;
        }

        // CRITERIO 2: Dataset masivo → SECUENCIAL
        if (estimacion.maximoPorProvincia > massiveThresholdPerProvince) {
            return EstrategiaProcessing.SECUENCIAL;
        }

        // CRITERIO 3: Dataset intermedio → HÍBRIDO
        return EstrategiaProcessing.HIBRIDO;
    }

    /**
     * PARALELO COMPLETO CON STREAMING - No acumula datos en memoria
     */
    private void procesarParaleloCompletoConStreaming(List<InfraccionesRepositoryImpl> repositories,
                                                      ParametrosFiltrosDTO filtros,
                                                      String nombreQuery,
                                                      Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("🚀 Ejecutando PARALELO COMPLETO CON STREAMING");
        long startTime = System.currentTimeMillis();

        // Configurar el procesador global para procesarLoteInmediato()
        this.procesadorLoteGlobal = procesadorLote;

        // Cambio crítico: usar runAsync en lugar de supplyAsync
        List<CompletableFuture<Void>> futures = repositories.stream()
                .map(repo -> CompletableFuture.runAsync(() -> {
                    // Llama al método que ya tienes modificado
                    ejecutarProvinciaCompleta(repo, filtros, nombreQuery);
                }, parallelExecutor))
                .collect(Collectors.toList());

        // Esperar que todas las provincias terminen
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Procesar registros restantes
        finalizarProcesamiento();

        long duration = System.currentTimeMillis() - startTime;
        log.info("✅ PARALELO COMPLETO CON STREAMING finalizado en {} ms", duration);
    }

    /**
     * Estrategias de procesamientos
     * */

    /**
     *  ESTRATEGIA 1: Paralelo Completo (datasets pequeños-medianos)
     */
    private void procesarParaleloCompleto(List<InfraccionesRepositoryImpl> repositories,
                                          ParametrosFiltrosDTO filtros,
                                          String nombreQuery,
                                          Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("🚀 Ejecutando PARALELO COMPLETO");
        long startTime = System.currentTimeMillis();

        List<CompletableFuture<List<Map<String, Object>>>> futures = repositories.stream()
                .map(repo -> CompletableFuture.supplyAsync(() -> {
                    return ejecutarProvinciaCompleta(repo, filtros, nombreQuery);
                }, parallelExecutor))
                .collect(Collectors.toList());

        // Procesar resultados conforme van llegando
        futures.forEach(future -> {
            try {
                List<Map<String, Object>> datos = future.get();
                if (!datos.isEmpty()) {
                    procesadorLote.accept(datos);
                }
            } catch (Exception e) {
                log.error("Error en procesamiento paralelo: {}", e.getMessage());
            }
        });

        long duration = System.currentTimeMillis() - startTime;
        log.info("✅ PARALELO COMPLETO finalizado en {} ms", duration);
    }

    /**
     * ESTRATEGIA 2: Híbrido Controlado (datasets intermedios)
     */
    private void procesarHibridoControlado(List<InfraccionesRepositoryImpl> repositories,
                                           ParametrosFiltrosDTO filtros,
                                           String nombreQuery,
                                           Consumer<List<Map<String, Object>>> procesadorLote,
                                           EstimacionDataset estimacion) {

        log.info("⚖️ Ejecutando HÍBRIDO CONTROLADO con {} provincias paralelas", maxParallelProvinces);
        long startTime = System.currentTimeMillis();

        // Procesar en grupos de N provincias simultáneas
        for (int i = 0; i < repositories.size(); i += maxParallelProvinces) {
            int endIndex = Math.min(i + maxParallelProvinces, repositories.size());
            List<InfraccionesRepositoryImpl> grupoRepositories = repositories.subList(i, endIndex);

            log.info("📦 Procesando grupo {}-{} de {} provincias",
                    i + 1, endIndex, repositories.size());

            List<CompletableFuture<List<Map<String, Object>>>> futures = grupoRepositories.stream()
                    .map(repo -> CompletableFuture.supplyAsync(() -> {
                        return ejecutarProvinciaCompleta(repo, filtros, nombreQuery);
                    }, parallelExecutor))
                    .collect(Collectors.toList());

            // Esperar que termine este grupo antes de continuar
            futures.forEach(future -> {
                try {
                    List<Map<String, Object>> datos = future.get();
                    if (!datos.isEmpty()) {
                        procesadorLote.accept(datos);
                    }
                } catch (Exception e) {
                    log.error("Error en grupo híbrido: {}", e.getMessage());
                }
            });

            // Pausa entre grupos para liberar memoria
            if (endIndex < repositories.size()) {
                try {
                    Thread.sleep(500);
                    System.gc(); // Sugerir limpieza
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("✅ HÍBRIDO CONTROLADO finalizado en {} ms", duration);
    }

    /**
     *  ESTRATEGIA 3: Secuencial Optimizado (datasets masivos)
     */
    /**
     * Procesa datos en lotes de manera eficiente con gestión de memoria mejorada de forma secuencial
     */
    public void procesarEnLotesFormaSecuencial(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            Consumer<List<Map<String, Object>>> procesadorLote) {

        log.info("Iniciando procesamiento en lotes para {} repositorios", repositories.size());

        int batchSize = calcularTamanoLoteOptimo(filtros);
        int totalProcesados = 0;
        int totalRepositorios = repositories.size();
        int repositorioActual = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            repositorioActual++;
            String provincia = repo.getProvincia();
            log.info("Procesando provincia {}/{}: {}", repositorioActual, totalRepositorios, provincia);

            int offset = 0;
            boolean hayMasDatos = true;
            int procesadosEnProvincia = 0;

            // ✅ CAMBIO CRÍTICO: Eliminar !esMemoriaCritica() de la condición
            while (hayMasDatos) {
                try {
                    // ✅ NUEVO: Manejo inteligente de memoria crítica
                    if (esMemoriaCritica()) {
                        log.warn("Memoria crítica detectada ({}%) - Reduciendo batch size y limpiando",
                                obtenerPorcentajeMemoriaUsada());

                        // Reducir batch size sobre la marcha
                        batchSize = Math.max(500, batchSize / 2);

                        // Limpieza agresiva
                        limpiarMemoriaAgresiva();

                        // Verificar si mejoró
                        if (esMemoriaCritica()) {
                            log.warn("Memoria sigue crítica, pero continuando con batch pequeño: {}", batchSize);
                        }
                    }

                    // Verificar memoria ANTES de procesar
                    if (esMemoriaAlta()) {
                        log.warn("Memoria alta detectada antes de procesar lote. Provincia: {}, Offset: {}",
                                provincia, offset);
                        pausaInteligente();
                    }

                    // Crear filtros específicos para este lote (usando batchSize actualizado)
                    ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtros, batchSize, offset);

                    // Ejecutar consulta para este lote
                    List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                    if (lote == null || lote.isEmpty()) {
                        log.debug("No hay más datos en provincia: {}, offset: {}", provincia, offset);
                        hayMasDatos = false;
                        break;
                    }

                    int tamanoLoteActual = lote.size();

                    // Agregar información de provincia a cada registro EN CHUNKS para evitar overhead
                    procesarProvinciaEnChunks(lote, provincia);

                    log.debug("Procesando lote: provincia={}, offset={}, tamaño={}, memoria={}%",
                            provincia, offset, tamanoLoteActual, obtenerPorcentajeMemoriaUsada());

                    // Procesar el lote en chunks pequeños con liberación frecuente
                    procesarLoteEnChunksConLiberacion(lote, procesadorLote);

                    // Actualizar contadores
                    procesadosEnProvincia += tamanoLoteActual;
                    totalProcesados += tamanoLoteActual;
                    offset += batchSize;

                    // Verificar si hay más datos
                    if (tamanoLoteActual < batchSize) {
                        log.debug("Lote incompleto ({} < {}), terminando provincia: {}",
                                tamanoLoteActual, batchSize, provincia);
                        hayMasDatos = false;
                    }

                    // Liberación EXPLÍCITA e INMEDIATA del lote
                    liberarLoteCompletamente(lote);

                    // Gestión de memoria periódica
                    gestionarMemoriaPeriodica(offset, batchSize, provincia);

                    // Log de progreso cada 10 lotes
                    if ((offset / batchSize) % 10 == 0) {
                        log.info("Progreso provincia {}: {} registros procesados, memoria: {}%",
                                provincia, procesadosEnProvincia, obtenerPorcentajeMemoriaUsada());
                    }

                } catch (OutOfMemoryError oom) {
                    log.error("OUT OF MEMORY en provincia {}, offset {}: {}", provincia, offset, oom.getMessage());
                    // Intentar recuperación
                    limpiarMemoriaAgresiva();
                    break; // Salir del while para esta provincia

                } catch (Exception e) {
                    log.error("Error procesando lote en provincia {}, offset {}: {}",
                            provincia, offset, e.getMessage(), e);

                    // Intentar continuar con siguiente lote si el error no es crítico
                    if (e.getMessage().contains("memoria") || e.getMessage().contains("memory")) {
                        limpiarMemoria();
                        offset += batchSize; // Saltar este lote problemático
                    } else {
                        break; // Salir del while para errores no relacionados con memoria
                    }
                }
            }

            // ✅ NUEVO: Limpieza agresiva OBLIGATORIA entre provincias
            log.info("Completada provincia {}: {} registros procesados, memoria: {}%",
                    provincia, procesadosEnProvincia, obtenerPorcentajeMemoriaUsada());

            // Limpieza FORZADA entre provincias
            // Reemplazar desde línea 409 hasta 428:
            if (repositorioActual < totalRepositorios) {
                log.info("Ejecutando limpieza COMPLETA entre provincias {}/{}...",
                        repositorioActual, totalRepositorios);

                double memoriaAntes = obtenerPorcentajeMemoriaUsada();

                // 1. Limpieza de memoria super agresiva
                for (int i = 0; i < 5; i++) {  // Era 3, ahora 5
                    System.gc();
                    System.runFinalization();
                    try {
                        Thread.sleep(1500);  // Era 1000ms, ahora 1500ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // 2. Limpieza específica de BD
                limpiarConexionesBD();

                // 3. Verificar si la memoria mejoró
                double memoriaDespues = obtenerPorcentajeMemoriaUsada();
                double memoriaLiberada = memoriaAntes - memoriaDespues;

                log.info("Limpieza entre provincias completada. Memoria liberada: {:.1f}%, Memoria actual: {:.1f}%",
                        memoriaLiberada, memoriaDespues);

                // 4. Si la memoria sigue alta, pausa más larga
                if (memoriaDespues > 80.0) {
                    log.warn("Memoria sigue alta ({}%), aplicando pausa extendida", memoriaDespues);
                    try {
                        Thread.sleep(3000);  // 3 segundos de pausa extendida
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                // 5. Pausa normal entre provincias
                pausaEntreProvincia();
            }
        }

        log.info("Procesamiento en lotes completado. Total procesados: {}, memoria final: {}%",
                totalProcesados, obtenerPorcentajeMemoriaUsada());
    }

    /**
     * 🏭 Ejecutar provincia completa (para estrategias paralelas)
     */
    private List<Map<String, Object>> ejecutarProvinciaCompleta(InfraccionesRepositoryImpl repo,
                                                                ParametrosFiltrosDTO filtros,
                                                                String nombreQuery) {
        String provincia = repo.getProvincia();

        // ¡CAMBIO CRÍTICO! No acumular, procesar por lotes
        List<Map<String, Object>> loteActual = new ArrayList<>();

        try {
            int offset = 0;
            boolean hayMasDatos = true;
            int maxLoteMemoria = 1000; // Máximo en memoria

            while (hayMasDatos) {
                ParametrosFiltrosDTO filtrosLote = filtros.toBuilder()
                        .limite(maxLoteMemoria)  // Lotes pequeños
                        .offset(offset)
                        .build();

                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    hayMasDatos = false;
                } else {
                    // Procesar INMEDIATAMENTE, no acumular
                    lote.forEach(registro -> registro.put("provincia", provincia));

                    // PROCESAR AHORA (no acumular)
                    procesarLoteInmediato(lote);

                    offset += maxLoteMemoria;
                    if (lote.size() < maxLoteMemoria) {
                        hayMasDatos = false;
                    }

                    // Limpiar memoria
                    lote.clear();
                }
            }

            return Collections.emptyList(); // No retornar datos acumulados

        } catch (Exception e) {
            log.error("❌ Error ejecutando provincia {}: {}", provincia, e.getMessage());
            return Collections.emptyList();
        }
    }

    private void procesarLoteInmediato(List<Map<String, Object>> lote) {
        if (lote == null || lote.isEmpty()) {
            return;
        }

        try {
            log.debug("Procesando lote inmediato de {} registros", lote.size());

            // Opción 1: Si tienes un Consumer configurado globalmente
            if (this.procesadorLoteGlobal != null) {
                this.procesadorLoteGlobal.accept(lote);
                return;
            }

            // Opción 2: Escribir directamente a archivo (si está configurado)
            if (this.archivoSalida != null) {
                escribirLoteAArchivo(lote);
                return;
            }

            // Opción 3: Almacenar temporalmente en cola thread-safe
            if (this.colaResultados != null) {
                this.colaResultados.addAll(lote);

                // Si la cola está llena, procesar un chunk
                if (this.colaResultados.size() >= this.chunkSize) {
                    procesarColaResultados();
                }
                return;
            }

            // Fallback: Log de advertencia
            log.warn("procesarLoteInmediato llamado pero no hay procesador configurado. {} registros perdidos",
                    lote.size());

        } catch (Exception e) {
            log.error("Error en procesarLoteInmediato: {}", e.getMessage(), e);
            throw new RuntimeException("Fallo en procesamiento inmediato", e);
        }
    }







    /**
     * Procesa el lote en chunks con liberación inmediata de memoria
     */
    private void procesarLoteEnChunksConLiberacion(List<Map<String, Object>> lote,
                                                   Consumer<List<Map<String, Object>>> procesadorLote) {

        int currentChunkSize = Math.min(chunkSize, lote.size());
        int chunksProcessed = 0;

        for (int i = 0; i < lote.size(); i += currentChunkSize) {
            int endIndex = Math.min(i + currentChunkSize, lote.size());

            // Crear sublista (vista, no copia completa)
            List<Map<String, Object>> chunk = lote.subList(i, endIndex);

            try {
                // Procesar el chunk
                procesadorLote.accept(chunk);
                chunksProcessed++;

                log.trace("Chunk {}/{} procesado, tamaño: {}",
                        chunksProcessed, (lote.size() + currentChunkSize - 1) / currentChunkSize, chunk.size());

            } catch (Exception e) {
                log.error("Error procesando chunk {}: {}", chunksProcessed, e.getMessage());
                throw e; // Re-lanzar para manejo en nivel superior
            }

            // Liberación EXPLÍCITA del chunk
            chunk.clear(); // Aunque sea una vista, limpiar referencias
            chunk = null;

            // Gestión de memoria cada 5 chunks
            if (chunksProcessed % 5 == 0) {
                if (esMemoriaAlta()) {
                    pausaMicro();
                }

                // Limpieza más agresiva cada 10 chunks
                if (chunksProcessed % 10 == 0) {
                    limpiarMemoria();

                    log.trace("Limpieza post-chunk {}, memoria: {}%",
                            chunksProcessed, obtenerPorcentajeMemoriaUsada());
                }
            }
        }
    }

    /**
     * Agrega provincia a registros en chunks para evitar overhead
     */
    private void procesarProvinciaEnChunks(List<Map<String, Object>> lote, String provincia) {
        int chunkSize = Math.min(100, lote.size()); // Chunks pequeños para esto

        for (int i = 0; i < lote.size(); i += chunkSize) {
            int endIndex = Math.min(i + chunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                lote.get(j).put("provincia", provincia);
            }

            // Micro-pausa cada chunk para permitir GC
            if (i > 0 && (i / chunkSize) % 20 == 0) {
                Thread.yield();
            }
        }
    }

    /**
     * Liberación completa y agresiva del lote
     */
    private void liberarLoteCompletamente(List<Map<String, Object>> lote) {
        try {
            // Limpiar cada mapa individual
            for (Map<String, Object> registro : lote) {
                if (registro != null) {
                    registro.clear();
                }
            }

            // Limpiar la lista
            lote.clear();

            // Anular referencia
            lote = null;

        } catch (Exception e) {
            log.warn("Error liberando lote: {}", e.getMessage());
        }
    }

    /**
     * Gestión de memoria periódica durante procesamiento
     */
    private void gestionarMemoriaPeriodica(int offset, int batchSize, String provincia) {
        // Cada 5 lotes
        if (offset % (batchSize * 5) == 0) {
            pausaInteligente();

            // Log de memoria cada 5 lotes
            log.debug("Memoria tras {} registros en {}: {}%",
                    offset, provincia, obtenerPorcentajeMemoriaUsada());
        }

        // Limpieza más agresiva cada 20 lotes
        if (offset % (batchSize * 20) == 0) {
            limpiarMemoria();
            log.debug("Limpieza agresiva en provincia {}, offset: {}", provincia, offset);
        }
    }

    /**
     * Pausa entre provincias para estabilizar memoria
     */
    private void pausaEntreProvincia() {
        try {
            log.debug("Pausa entre provincias, memoria antes: {}%", obtenerPorcentajeMemoriaUsada());

            // Limpieza pre-pausa
            limpiarMemoria();

            // Pausa para permitir estabilización
            Thread.sleep(100);

            log.debug("Memoria después de pausa: {}%", obtenerPorcentajeMemoriaUsada());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Limpieza agresiva de memoria para situaciones críticas
     */
    /**
     * Limpieza agresiva de memoria para situaciones críticas
     */
    private void limpiarMemoriaAgresiva() {
        log.warn("Ejecutando limpieza SUPER agresiva de memoria...");

        long memoriaAntes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // Múltiples pasadas más agresivas
        for (int i = 0; i < 5; i++) {  // Era 3, ahora 5
            System.gc();
            System.runFinalization();
            try {
                Thread.sleep(800);  // Era 200ms, ahora 800ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Limpieza final más agresiva
        System.gc();
        Thread.yield();

        long memoriaDespues = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoriaLiberada = memoriaAntes - memoriaDespues;

        log.warn("Limpieza agresiva completada. Liberados: {}MB, Memoria actual: {}%",
                memoriaLiberada / 1024 / 1024, obtenerPorcentajeMemoriaUsada());
    }

    /**
     * Obtiene porcentaje de memoria usada para logs
     */
    private double obtenerPorcentajeMemoriaUsada() {
        Runtime runtime = Runtime.getRuntime();
        long memoriaUsada = runtime.totalMemory() - runtime.freeMemory();
        return (double) memoriaUsada / runtime.totalMemory() * 100;
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
          * Calcula el tamaño óptimo de lote usando el nuevo método
 */
    private int calcularTamanoLoteOptimo(ParametrosFiltrosDTO filtros) {
        // Usar el límite efectivo del DTO actualizado
        int batchSizeBase = filtros.getLimiteEfectivo();

        // Si es muy pequeño, usar el defaultBatchSize
        if (batchSizeBase < 1000) {
            batchSizeBase = defaultBatchSize;
        }

        log.info("Límite base calculado: {} (info: {})",
                batchSizeBase, filtros.getInfoPaginacion());

        // Considerar memoria disponible
        Runtime runtime = Runtime.getRuntime();
        long memoriaLibre = runtime.freeMemory();
        long memoriaTotal = runtime.totalMemory();
        double porcentajeLibre = (double) memoriaLibre / memoriaTotal;

        int batchSizeOptimo = batchSizeBase;

        // Ajustar según memoria disponible
        if (porcentajeLibre < 0.20) {
            batchSizeOptimo = Math.max(1000, batchSizeBase / 4);
            log.warn("Memoria muy baja ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        } else if (porcentajeLibre < 0.30) {
            batchSizeOptimo = Math.max(2000, batchSizeBase / 2);
            log.info("Memoria baja ({}%), reduciendo lote a {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        } else if (porcentajeLibre > 0.70) {
            // Memoria abundante, mantener o aumentar
            batchSizeOptimo = Math.min(batchSizeBase, 10000); // Máximo 10k
            log.info("Memoria abundante ({}%), usando lote de {}",
                    String.format("%.1f%%", porcentajeLibre * 100), batchSizeOptimo);
        }

        log.info("Tamaño de lote FINAL: {} (base: {}, memoria: {}%)",
                batchSizeOptimo, batchSizeBase, String.format("%.1f", porcentajeLibre * 100));

        return batchSizeOptimo;
    }


    /**
     * Crea filtros para lote usando el DTO actualizado
     */
    private ParametrosFiltrosDTO crearFiltrosParaLote(ParametrosFiltrosDTO filtrosOriginales,
                                                      int batchSize, int offset) {

        log.debug("Creando filtros - batchSize: {}, offset: {}, original: {}",
                batchSize, offset, filtrosOriginales.getInfoPaginacion());

        ParametrosFiltrosDTO filtrosLote = filtrosOriginales.toBuilder()
                .limite(batchSize)           // Establecer límite explícito
                .offset(offset)              // Establecer offset explícito
                .pagina(null)                // Limpiar página para evitar conflictos
                .tamanoPagina(null)          // Limpiar tamaño página
                .build();

        // Validar que se creó correctamente
        if (!filtrosLote.validarPaginacion()) {
            log.warn("Filtros de lote inválidos: {}", filtrosLote.getInfoPaginacion());
        }

        log.debug("Filtros de lote creados: {}", filtrosLote.getInfoPaginacion());

        return filtrosLote;
    }

    /**
     * Escribe lote directamente al archivo CSV
     */
    private void escribirLoteAArchivo(List<Map<String, Object>> lote) throws IOException {
        if (archivoSalida == null) {
            throw new IllegalStateException("Archivo de salida no configurado");
        }

        for (Map<String, Object> registro : lote) {
            StringBuilder linea = new StringBuilder();
            boolean first = true;

            // Escribir valores separados por comas
            for (Object valor : registro.values()) {
                if (!first) linea.append(",");

                String valorStr = valor != null ? valor.toString() : "";
                // Escapar comillas y comas
                if (valorStr.contains("\"") || valorStr.contains(",")) {
                    valorStr = "\"" + valorStr.replace("\"", "\"\"") + "\"";
                }
                linea.append(valorStr);
                first = false;
            }

            archivoSalida.write(linea.toString());
            archivoSalida.newLine();
        }

        archivoSalida.flush(); // Forzar escritura
    }

    /**
     * Procesa la cola de resultados cuando está llena
     */
    private void procesarColaResultados() {
        List<Map<String, Object>> loteParaProcesar = new ArrayList<>();

        // Extraer elementos de la cola
        for (int i = 0; i < chunkSize && !colaResultados.isEmpty(); i++) {
            Map<String, Object> registro = colaResultados.poll();
            if (registro != null) {
                loteParaProcesar.add(registro);
            }
        }

        if (!loteParaProcesar.isEmpty() && procesadorLoteGlobal != null) {
            procesadorLoteGlobal.accept(loteParaProcesar);
        }
    }

    /**
     * Método para finalizar y procesar registros restantes
     */
    public void finalizarProcesamiento() {
        try {
            // Procesar registros restantes en cola
            if (!colaResultados.isEmpty()) {
                List<Map<String, Object>> restantes = new ArrayList<>(colaResultados);
                colaResultados.clear();
                if (procesadorLoteGlobal != null) {
                    procesadorLoteGlobal.accept(restantes);
                }
            }

            // Cerrar archivo si existe
            if (archivoSalida != null) {
                archivoSalida.close();
                archivoSalida = null;
            }

        } catch (Exception e) {
            log.error("Error finalizando procesamiento: {}", e.getMessage(), e);
        }
    }

    /**
     * Versión modificada de ejecutarProvinciaCompleta() que usa procesamiento inmediato
     */
    private List<Map<String, Object>> ejecutarProvinciaCompletaConStreaming(InfraccionesRepositoryImpl repo,
                                                                            ParametrosFiltrosDTO filtros,
                                                                            String nombreQuery) {
        String provincia = repo.getProvincia();

        try {
            int offset = 0;
            boolean hayMasDatos = true;
            int maxLoteMemoria = 1000; // Mantener lotes pequeños
            int totalProcesados = 0;

            while (hayMasDatos) {
                // Verificar memoria antes de procesar
                if (esMemoriaCritica()) {
                    log.warn("Memoria crítica en provincia {}, pausando...", provincia);
                    esperarLiberacionMemoria();
                }

                ParametrosFiltrosDTO filtrosLote = filtros.toBuilder()
                        .limite(maxLoteMemoria)
                        .offset(offset)
                        .build();

                List<Map<String, Object>> lote = repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                if (lote == null || lote.isEmpty()) {
                    hayMasDatos = false;
                } else {
                    // Agregar provincia a cada registro
                    lote.forEach(registro -> registro.put("provincia", provincia));

                    // PROCESAR INMEDIATAMENTE (no acumular)
                    procesarLoteInmediato(lote);

                    totalProcesados += lote.size();
                    offset += maxLoteMemoria;

                    if (lote.size() < maxLoteMemoria) {
                        hayMasDatos = false;
                    }

                    // Limpiar memoria inmediatamente
                    lote.clear();
                    lote = null;

                    // Log de progreso cada 10 lotes
                    if ((offset / maxLoteMemoria) % 10 == 0) {
                        log.debug("Provincia {}: {} registros procesados", provincia, totalProcesados);
                    }
                }
            }

            log.info("✅ Provincia {} completada: {} registros procesados", provincia, totalProcesados);
            return Collections.emptyList(); // No retornar datos acumulados

        } catch (Exception e) {
            log.error("❌ Error ejecutando provincia {}: {}", provincia, e.getMessage());
            return Collections.emptyList();
        }
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
     * Limpieza específica de conexiones de base de datos
     */
    private void limpiarConexionesBD() {
        try {
            log.debug("Ejecutando limpieza de conexiones BD...");

            // Forzar limpieza de statement cache y metadatos
            System.runFinalization();

            // Pausa para permitir que las conexiones se liberen
            Thread.sleep(500);

            log.debug("Limpieza de conexiones BD completada");

        } catch (Exception e) {
            log.warn("Error en limpieza de conexiones BD: {}", e.getMessage());
        }
    }

}