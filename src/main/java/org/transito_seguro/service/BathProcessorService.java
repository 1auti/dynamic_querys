package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.BathResultDTO;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.TaskProgress;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BathProcessorService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    // Configuración por defecto (puede venir de application.yml)
    @Value("${app.batch.size:1000}")
    private int batchSize;

    @Value("${app.batch.max-retries:3}")
    private int maxRetries;

    @Value("${app.batch.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryPerBatchMB;

    /**
     * Procesa una consulta por lotes
     */
    public List<Map<String, Object>> procesarPorLotes(
            ConsultaQueryDTO consulta,
            Consumer<TaskProgress> progressCallback) {

        log.info("Iniciando procesamiento por lotes - Batch size: {}", batchSize);

        // 1. Determinar repositorios a usar
        List<InfraccionesRepository> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            throw new RuntimeException("No hay repositorios disponibles para la consulta");
        }

        // 2. Estimar total de registros y lotes
        EstimacionConsulta estimacion = estimarConsulta(repositories, consulta.getParametrosFiltros());

        log.info("Estimación: {} provincias, ~{} registros, ~{} lotes",
                repositories.size(), estimacion.getRegistrosEstimados(), estimacion.getLotesEstimados());

        // 3. Procesar cada provincia en paralelo controlado
        List<Map<String, Object>> resultadosCombinados = new ArrayList<>();
        Queue<BathResultDTO> resultadosLotes = new ConcurrentLinkedQueue<>();

        int provinciaActual = 0;
        for (InfraccionesRepository repository : repositories) {
            String provincia = ((InfraccionesRepositoryImpl) repository).getProvincia();
            provinciaActual++;

            log.info("Procesando provincia {}/{}: {}", provinciaActual, repositories.size(), provincia);

            // Actualizar progreso - iniciando provincia
            actualizarProgreso(progressCallback, TaskProgress.builder()
                    .porcentajeTotal(((double) (provinciaActual - 1) / repositories.size()) * 100)
                    .provinciaActual(provincia)
                    .loteActual(0)
                    .totalLotes(estimacion.getLotesEstimados())
                    .registrosProcesados((long) resultadosCombinados.size())
                    .mensaje(String.format("Procesando provincia %s (%d/%d)",
                            provincia, provinciaActual, repositories.size()))
                    .build());

            try {
                // Procesar provincia por lotes
                List<Map<String, Object>> resultadosProvincia = procesarProvinciaPorLotes(
                        repository,
                        consulta.getParametrosFiltros(),
                        provincia,
                        progressCallback,
                        provinciaActual,
                        repositories.size(),
                        resultadosCombinados.size()
                );

                resultadosCombinados.addAll(resultadosProvincia);
                log.info("Provincia {} completada: {} registros", provincia, resultadosProvincia.size());

            } catch (Exception e) {
                log.error("Error procesando provincia {}: {}", provincia, e.getMessage(), e);

                // Continuar con otras provincias
                resultadosLotes.add(BathResultDTO.error(provincia, 0, e.getMessage()));
            }
        }

        // 4. Progreso final
        actualizarProgreso(progressCallback, TaskProgress.builder()
                .porcentajeTotal(100.0)
                .registrosProcesados((long) resultadosCombinados.size())
                .mensaje("Procesamiento completado - " + resultadosCombinados.size() + " registros")
                .build());

        log.info("Procesamiento por lotes completado: {} registros totales", resultadosCombinados.size());
        return resultadosCombinados;
    }

    /**
     * Procesa una provincia específica por lotes
     */
    private List<Map<String, Object>> procesarProvinciaPorLotes(
            InfraccionesRepository repository,
            ParametrosFiltrosDTO filtrosOriginales,
            String provincia,
            Consumer<TaskProgress> progressCallback,
            int provinciaActual,
            int totalProvincias,
            int registrosPrevios) {

        List<Map<String, Object>> resultadosProvincia = new ArrayList<>();
        int offset = 0;
        int numeroBatch = 1;
        boolean hayMasRegistros = true;

        long inicioTiempo = System.currentTimeMillis();

        while (hayMasRegistros) {
            try {
                log.debug("Procesando lote {} de provincia {} (offset: {})", numeroBatch, provincia, offset);

                // Crear filtros para este lote específico
                ParametrosFiltrosDTO filtrosLote = crearFiltrosParaLote(filtrosOriginales, offset, batchSize);

                // Ejecutar consulta del lote con timeout
                List<Map<String, Object>> lote = ejecutarLoteConTimeout(
                        repository, filtrosLote, provincia, numeroBatch);

                if (lote.isEmpty()) {
                    // No hay más registros
                    hayMasRegistros = false;
                    log.debug("Provincia {} completada - No más registros en lote {}", provincia, numeroBatch);
                } else {
                    // Procesar lote
                    resultadosProvincia.addAll(lote);

                    // Calcular progreso
                    double progresoProvincia = (double) provinciaActual / totalProvincias;
                    double progresoGeneral = ((provinciaActual - 1.0) / totalProvincias) * 100 +
                            (progresoProvincia * 10); // Estimación dentro de la provincia

                    // Actualizar progreso
                    actualizarProgreso(progressCallback, TaskProgress.builder()
                            .porcentajeTotal(Math.min(progresoGeneral, 99.0)) // Nunca 100% hasta el final
                            .provinciaActual(provincia)
                            .loteActual(numeroBatch)
                            .registrosProcesados((long) (registrosPrevios + resultadosProvincia.size()))
                            .velocidadProcesamiento(calcularVelocidadProcesamiento(inicioTiempo, resultadosProvincia.size()))
                            .mensaje(String.format("Procesando %s - Lote %d (%d registros)",
                                    provincia, numeroBatch, lote.size()))
                            .build());

                    // Preparar siguiente lote
                    offset += batchSize;
                    numeroBatch++;

                    // Control de memoria - si el resultado es muy grande, hacer limpieza
                    if (resultadosProvincia.size() % (batchSize * 10) == 0) {
                        System.gc(); // Sugerir garbage collection cada 10 lotes
                    }
                }

            } catch (Exception e) {
                log.error("Error en lote {} de provincia {}: {}", numeroBatch, provincia, e.getMessage());

                // Reintentar si no se alcanzó el límite
                if (numeroBatch <= maxRetries) {
                    log.warn("Reintentando lote {} de provincia {} ({}/{})",
                            numeroBatch, provincia, numeroBatch, maxRetries);
                    continue;
                } else {
                    log.error("Máximo de reintentos alcanzado para provincia {}", provincia);
                    break; // Salir del while loop
                }
            }
        }

        log.info("Provincia {} procesada: {} registros en {} lotes",
                provincia, resultadosProvincia.size(), numeroBatch - 1);

        return resultadosProvincia;
    }

    /**
     * Ejecuta un lote con timeout y manejo de errores
     */
    private List<Map<String, Object>> ejecutarLoteConTimeout(
            InfraccionesRepository repository,
            ParametrosFiltrosDTO filtros,
            String provincia,
            int numeroBatch) {

        try {
            long inicio = System.currentTimeMillis();

            // Ejecutar consulta (el timeout debería manejarse en el nivel de BD/connection pool)
            List<Map<String, Object>> resultado =
                    ((InfraccionesRepositoryImpl) repository).ejecutarQueryConFiltros(
                            "consultar_personas_juridicas.sql", filtros);

            long duracion = System.currentTimeMillis() - inicio;
            log.debug("Lote {} de {} ejecutado en {}ms: {} registros",
                    numeroBatch, provincia, duracion, resultado.size());

            // Agregar metadata de provincia si no existe
            resultado.forEach(registro -> {
                if (!registro.containsKey("provincia_origen")) {
                    registro.put("provincia_origen", provincia);
                }
                registro.put("lote_numero", numeroBatch);
            });

            return resultado;

        } catch (Exception e) {
            log.error("Error ejecutando lote {} de {}: {}", numeroBatch, provincia, e.getMessage());
            throw new RuntimeException("Error en lote " + numeroBatch + " de " + provincia + ": " + e.getMessage(), e);
        }
    }

    /**
     * Crea filtros específicos para un lote
     */
    private ParametrosFiltrosDTO crearFiltrosParaLote(ParametrosFiltrosDTO filtrosOriginales, int offset, int limite) {
        return filtrosOriginales.toBuilder()
                .limiteMaximo(limite)
                .pagina(offset / limite)
                .tamanoPagina(limite)
                .build();
    }

    /**
     * Determina repositorios a usar según filtros
     */
    private List<InfraccionesRepository> determinarRepositories(ParametrosFiltrosDTO filtros) {
        if (filtros.debeConsultarTodasLasBDS()) {
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        } else if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            return filtros.getBaseDatos().stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(repositoryFactory::getRepository)
                    .collect(Collectors.toList());
        } else {
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        }
    }

    /**
     * Estima el número de registros y lotes de una consulta
     */
    private EstimacionConsulta estimarConsulta(List<InfraccionesRepository> repositories, ParametrosFiltrosDTO filtros) {
        // Estimación simple - podría mejorarse con queries COUNT específicas
        int numProvincias = repositories.size();
        int registrosEstimadosPorProvincia = 10000; // Estimación conservadora

        // Ajustar estimación según filtros
        if (filtros.tieneRangoFechas() || filtros.debeUsarFechaEspecifica()) {
            registrosEstimadosPorProvincia = 5000; // Menos registros con filtro de fecha
        }

        int registrosEstimados = numProvincias * registrosEstimadosPorProvincia;
        int lotesEstimados = (registrosEstimados / batchSize) + 1;

        return new EstimacionConsulta(registrosEstimados, lotesEstimados);
    }

    /**
     * Calcula la velocidad de procesamiento
     */
    private Double calcularVelocidadProcesamiento(long inicioTiempo, int registrosProcesados) {
        long duracionMs = System.currentTimeMillis() - inicioTiempo;
        if (duracionMs == 0) return 0.0;

        double duracionSegundos = duracionMs / 1000.0;
        return registrosProcesados / duracionSegundos;
    }

    /**
     * Wrapper para actualizar progreso de forma segura
     */
    private void actualizarProgreso(Consumer<TaskProgress> callback, TaskProgress progreso) {
        try {
            if (callback != null) {
                callback.accept(progreso);
            }
        } catch (Exception e) {
            log.warn("Error actualizando progreso: {}", e.getMessage());
        }
    }

    /**
     * Clase interna para estimación
     */
    private static class EstimacionConsulta {
        private final int registrosEstimados;
        private final int lotesEstimados;

        public EstimacionConsulta(int registrosEstimados, int lotesEstimados) {
            this.registrosEstimados = registrosEstimados;
            this.lotesEstimados = lotesEstimados;
        }

        public int getRegistrosEstimados() { return registrosEstimados; }
        public int getLotesEstimados() { return lotesEstimados; }
    }
}