package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

@Slf4j
@Service
public class InfraccionesService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private ConsultaValidator validator;

    @Autowired
    private FormatoConverter formatoConverter;

    @Autowired
    private BatchProcessor batchProcessor;

    @Autowired
    private StreamingFormatoConverter streamingConverter;

    @Value("${app.limits.max-records-sync:1000}")
    private int maxRecordsSincrono;

    @Value("${app.limits.max-records-total:50000}")
    private int maxRecordsTotal;

    @Value("${app.batch.max-memory-per-batch:50}")
    private int maxMemoryMB;

    @Value("${app.async.thread-pool-size:10}")
    private int threadPoolSize;

    private final Executor executor;

    // Constructor para inicializar el thread pool
    public InfraccionesService() {
        this.executor = Executors.newFixedThreadPool(threadPoolSize > 0 ? threadPoolSize : 10);
    }

    @PreDestroy
    public void cleanup() {
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).shutdown();
        }
    }

    /**
     * Método principal - decide entre procesamiento síncrono o por lotes
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    /**
     * Método mejorado que decide automáticamente el tipo de procesamiento
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        log.info("Iniciando consulta de infracciones con query: {}", nombreQuery);

        // 1. Validar consulta
        validator.validarConsulta(consulta);

        // 2. Determinar repositorios
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());

        if (repositories.isEmpty()) {
            log.warn("No se encontraron repositorios válidos para la consulta");
            return formatoConverter.convertir(Collections.emptyList(),
                    consulta.getFormato() != null ? consulta.getFormato() : "json");

        }

        // 3. Estimar tamaño de resultado
        int estimacionRegistros = estimarTamanoResultado(repositories, consulta.getParametrosFiltros(), nombreQuery);
        log.info("Estimación de registros: {}", estimacionRegistros);

        // 4. Decidir estrategia de procesamiento
        if (estimacionRegistros <= maxRecordsSincrono) {
            log.info("Usando procesamiento síncrono (estimación: {} registros)", estimacionRegistros);
            return procesarSincrono(repositories, consulta, nombreQuery);
        } else {
            log.info("Usando procesamiento por lotes (estimación: {} registros)", estimacionRegistros);
            return procesarEnLotes(repositories, consulta, nombreQuery);
        }
    }

    /**
     * Procesamiento síncrono tradicional (para consultas pequeñas)
     */
    private Object procesarSincrono(List<InfraccionesRepositoryImpl> repositories,
                                    ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        List<Map<String, Object>> resultadosCombinados = repositories.parallelStream()
                .flatMap(repo -> {
                    try {
                        String provincia = repo.getProvincia();
                        List<Map<String, Object>> resultado = repo.ejecutarQueryConFiltros(
                                nombreQuery, consulta.getParametrosFiltros()
                        );

                        // Agregar provincia a cada registro
                        resultado.forEach(registro -> registro.put("provincia_origen", provincia));
                        return resultado.stream();

                    } catch (Exception e) {
                        log.error("Error en provincia {}: {}", repo.getProvincia(), e.getMessage());
                        return java.util.stream.Stream.empty();
                    }
                })
                .collect(Collectors.toList());

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    /**
     * Procesamiento por lotes con streaming (para consultas grandes)
     */
    private Object procesarEnLotes(List<InfraccionesRepositoryImpl> repositories,
                                   ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {

        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        StreamingFormatoConverter.StreamingContext context = null;

        try {
            // Crear stream de salida en memoria con límite configurable
            StreamingFormatoConverter.LimitedByteArrayOutputStream outputStream =
                    new StreamingFormatoConverter.LimitedByteArrayOutputStream(maxMemoryMB * 1024 * 1024);

            // Inicializar streaming
            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming");
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            // Procesar en lotes con chunks
            batchProcessor.procesarEnLotes(
                    repositories,
                    consulta.getParametrosFiltros(),
                    nombreQuery,
                    lote -> {
                        try {
                            if (lote != null && !lote.isEmpty()) {
                                streamingConverter.procesarLoteStreaming(finalContext, lote);

                                // Limpiar memoria periódicamente
                                if (finalContext.getTotalRegistros() % 5000 == 0) {
                                    batchProcessor.limpiarMemoria();
                                }
                            }
                        } catch (Exception e) {
                            log.error("Error procesando lote en streaming: {}", e.getMessage(), e);
                            throw new RuntimeException("Error en procesamiento por lotes", e);
                        }
                    }
            );

            // Finalizar streaming
            streamingConverter.finalizarStreaming(context);

            // Retornar según formato
            switch (formato.toLowerCase()) {
                case "json":
                case "csv":
                    return outputStream.toString("UTF-8");
                case "excel":
                    return outputStream.toByteArray();
                default:
                    throw new IllegalArgumentException("Formato no soportado: " + formato);
            }

        } catch (Exception e) {
            log.error("Error en procesamiento por lotes: {}", e.getMessage(), e);

            // Intentar cerrar el contexto si existe
            if (context != null) {
                try {
                    streamingConverter.finalizarStreaming(context);
                } catch (Exception cleanupException) {
                    log.warn("Error cerrando contexto de streaming: {}", cleanupException.getMessage());
                }
            }

            throw new RuntimeException("Error procesando consulta por lotes", e);
        }
    }

    /**
     * Versión que retorna ResponseEntity para archivos grandes
     */
    public ResponseEntity<byte[]> consultarInfraccionesComoArchivo(ConsultaQueryDTO consulta, String nombreQuery)
            throws ValidationException {

        log.info("Generando archivo para consulta con query: {}", nombreQuery);

        validator.validarConsulta(consulta);
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(consulta.getParametrosFiltros());
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";

        StreamingFormatoConverter.StreamingContext context = null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming para archivo");
            }

            final StreamingFormatoConverter.StreamingContext finalContext = context;

            batchProcessor.procesarEnLotes(
                    repositories,
                    consulta.getParametrosFiltros(),
                    nombreQuery,
                    lote -> {
                        try {
                            if (lote != null && !lote.isEmpty()) {
                                streamingConverter.procesarLoteStreaming(finalContext, lote);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error procesando lote para archivo", e);
                        }
                    }
            );

            streamingConverter.finalizarStreaming(context);

            // Preparar headers HTTP
            HttpHeaders headers = new HttpHeaders();
            String filename = generarNombreArchivo(formato);
            headers.setContentDispositionFormData("attachment", filename);

            MediaType mediaType;
            switch (formato.toLowerCase()) {
                case "csv":
                    mediaType = MediaType.parseMediaType("text/csv");
                    break;
                case "excel":
                    mediaType = MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                    break;
                case "json":
                default:
                    mediaType = MediaType.APPLICATION_JSON;
                    break;
            }
            headers.setContentType(mediaType);

            byte[] responseData = outputStream.toByteArray();
            log.info("Archivo generado exitosamente: {} bytes", responseData.length);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(responseData);

        } catch (Exception e) {
            log.error("Error generando archivo: {}", e.getMessage(), e);

            // Limpiar contexto si existe
            if (context != null) {
                try {
                    streamingConverter.finalizarStreaming(context);
                } catch (Exception cleanupException) {
                    log.warn("Error limpiando contexto: {}", cleanupException.getMessage());
                }
            }

            throw new RuntimeException("Error generando archivo", e);
        }
    }

    /**
     * Estima el tamaño del resultado antes de ejecutar la consulta completa
     * CORREGIDO: Usa el nombreQuery recibido como parámetro
     */
    private int estimarTamanoResultado(List<InfraccionesRepositoryImpl> repositories,
                                       ParametrosFiltrosDTO filtros, String nombreQuery) {
        // Crear filtros de prueba con límite pequeño
        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(10)
                .pagina(0)
                .build();

        int totalEstimado = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            try {
                // CORREGIDO: Usar nombreQuery en lugar de hardcodear
                List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);

                // Si obtuvimos 10 registros, probablemente hay muchos más
                if (muestra.size() >= 10) {
                    totalEstimado += 1000; // Estimación conservadora
                } else if (muestra.size() > 0) {
                    totalEstimado += muestra.size() * 10; // Multiplicar por factor de escala
                }
                // Si muestra.size() == 0, no agregamos nada

            } catch (Exception e) {
                log.warn("No se pudo estimar para provincia {}: {}", repo.getProvincia(), e.getMessage());
                totalEstimado += 500; // Estimación por defecto
            }
        }

        return Math.min(totalEstimado, maxRecordsTotal);
    }

    private String generarNombreArchivo(String formato) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("infracciones_%s.%s", timestamp, formato.toLowerCase());
    }

    private List<InfraccionesRepositoryImpl> determinarRepositories(ParametrosFiltrosDTO filtros) {
        if (filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()) {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        } else if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            return filtros.getBaseDatos().stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(provincia -> (InfraccionesRepositoryImpl) repositoryFactory.getRepository(provincia))
                    .collect(Collectors.toList());
        } else {
            // Usar todas las BDS por defecto si no se especifica nada
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Método específico para procesar consultas grandes de forma asíncrona
     */
    public CompletableFuture<String> consultarInfraccionesAsincrono(ConsultaQueryDTO consulta, String nombreQuery) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Object resultado = consultarInfracciones(consulta, nombreQuery);
                return resultado instanceof String ? (String) resultado : resultado.toString();
            } catch (Exception e) {
                log.error("Error en consulta asíncrona: {}", e.getMessage(), e);
                throw new RuntimeException("Error en consulta asíncrona", e);
            }
        }, executor);
    }

    /**
     * Obtiene estadísticas de memoria y procesamiento
     */
    public Map<String, Object> obtenerEstadisticasProcesamiento() {
        Map<String, Object> estadisticas = new HashMap<>();

        // Estadísticas de memoria (usando método mejorado si existe)
        try {
            estadisticas.putAll(batchProcessor.obtenerEstadisticasMemoriaExtendidas());
        } catch (Exception e) {
            // Fallback al método original
            estadisticas.putAll(batchProcessor.obtenerEstadisticasMemoria());
        }

        // Configuración de límites
        estadisticas.put("max_records_sincrono", maxRecordsSincrono);
        estadisticas.put("max_records_total", maxRecordsTotal);
        estadisticas.put("max_memory_mb", maxMemoryMB);
        estadisticas.put("thread_pool_size", threadPoolSize);

        // Estado de repositorios
        estadisticas.put("repositorios_disponibles", repositoryFactory.getProvinciasSoportadas().size());
        estadisticas.put("provincias_soportadas", repositoryFactory.getProvinciasSoportadas());

        return estadisticas;
    }

    // =============== MÉTODOS ESPECÍFICOS DE CONSULTA (CON BATCH SUPPORT) ===============

    public Object consultarPersonasJuridicas(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    public Object consultarReporteGeneral(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_infracciones_general.sql");
    }

    public Object consultarInfraccionesPorEquipos(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_infracciones_por_equipos.sql");
    }

    public Object consultarVehiculosPorMunicipio(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_vehiculos_por_municipio.sql");
    }

    public Object consultarRadarFijoPorEquipo(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_radar_fijo_por_equipo.sql");
    }

    public Object consultarSemaforoPorEquipo(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_semaforo_por_equipo.sql");
    }

    // =============== VERSIONES PARA DESCARGA DE ARCHIVOS ===============

    public ResponseEntity<byte[]> descargarPersonasJuridicas(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfraccionesComoArchivo(consulta, "consultar_personas_juridicas.sql");
    }

    public ResponseEntity<byte[]> descargarReporteGeneral(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfraccionesComoArchivo(consulta, "reporte_infracciones_general.sql");
    }

    public ResponseEntity<byte[]> descargarInfraccionesPorEquipos(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfraccionesComoArchivo(consulta, "reporte_infracciones_por_equipos.sql");
    }

    // =============== MÉTODOS DE UTILIDAD ===============

    public Map<String, Object> obtenerEstadisticasConsulta(ParametrosFiltrosDTO filtros) {
        List<InfraccionesRepositoryImpl> repositories = determinarRepositories(filtros);

        List<String> provinciasSeleccionadas = repositories.stream()
                .map(InfraccionesRepositoryImpl::getProvincia)
                .collect(Collectors.toList());

        // CORREGIDO: Usar query por defecto para estimación
        int estimacionRegistros = estimarTamanoResultado(repositories, filtros, "consultar_personas_juridicas.sql");
        String estrategiaProcesamiento = estimacionRegistros <= maxRecordsSincrono ? "SINCRONO" : "LOTES";

        Map<String, Object> estadisticas = new HashMap<>();
        estadisticas.put("repositoriosDisponibles", repositoryFactory.getProvinciasSoportadas().size());
        estadisticas.put("repositoriosSeleccionados", repositories.size());
        estadisticas.put("provinciasSeleccionadas", provinciasSeleccionadas);
        estadisticas.put("estimacionRegistros", estimacionRegistros);
        estadisticas.put("estrategiaProcesamiento", estrategiaProcesamiento);

        // Usar estadísticas de memoria
        try {
            estadisticas.putAll(batchProcessor.obtenerEstadisticasMemoriaExtendidas());
        } catch (Exception e) {
            estadisticas.putAll(batchProcessor.obtenerEstadisticasMemoria());
        }

        return estadisticas;
    }

    public Map<String, Boolean> validarConectividadRepositorios() {
        return repositoryFactory.getAllRepositories().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                ParametrosFiltrosDTO filtrosTest = ParametrosFiltrosDTO.builder()
                                        .limite(1)
                                        .build();

                                List<Map<String, Object>> resultado = ((InfraccionesRepositoryImpl) entry.getValue())
                                        .ejecutarQueryConFiltros("SELECT 1 as test", filtrosTest);

                                return resultado != null;
                            } catch (Exception e) {
                                log.warn("Repositorio {} no disponible: {}", entry.getKey(), e.getMessage());
                                return false;
                            }
                        }
                ));
    }

    /**
     * Método utilitario para construir consultas con filtros específicos
     */
    public ConsultaQueryDTO.ConsultaQueryDTOBuilder crearConsultaBase() {
        return ConsultaQueryDTO.builder()
                .formato("json")
                .parametrosFiltros(
                        ParametrosFiltrosDTO.builder()
                                .usarTodasLasBDS(true)
                                .pagina(0)
                                .limite(50)
                                .build()
                );
    }
}