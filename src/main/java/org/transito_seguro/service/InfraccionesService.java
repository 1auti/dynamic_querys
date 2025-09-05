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
import org.transito_seguro.enums.Consultas;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import javax.annotation.PreDestroy;
import javax.xml.bind.ValidationException;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
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

    /**
     * Mapeo centralizado de tipos de consulta a archivos SQL
     */
    private static final Map<String, String> QUERY_MAPPING = Consultas.getMapeoCompleto();


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

    // =============== MÉTODOS PÚBLICOS PRINCIPALES ===============

    /**
     * Método principal - decide entre procesamiento síncrono o por lotes
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    /**
     * Método genérico que reemplaza todos los métodos específicos
     */
    public Object ejecutarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfracciones(consulta, nombreQuery);
    }

    /**
     * Método genérico para descarga de archivos
     */
    public ResponseEntity<byte[]> descargarConsultaPorTipo(String tipoConsulta, ConsultaQueryDTO consulta) throws ValidationException {
        String nombreQuery = QUERY_MAPPING.get(tipoConsulta);
        if (nombreQuery == null) {
            throw new IllegalArgumentException("Tipo de consulta no soportado: " + tipoConsulta +
                    ". Tipos válidos: " + QUERY_MAPPING.keySet());
        }
        return consultarInfraccionesComoArchivo(consulta, nombreQuery);
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

    // =============== MÉTODOS PRIVADOS DE PROCESAMIENTO ===============

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
            File tempFile = File.createTempFile("infracciones_", "." + formato);
            FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
            BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream, 8192);

            context = streamingConverter.inicializarStreaming(formato, outputStream);

            if (context == null) {
                throw new RuntimeException("No se pudo inicializar el contexto de streaming");
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

            streamingConverter.finalizarStreaming(context);

            outputStream.close();
            byte[] fileData = Files.readAllBytes(tempFile.toPath());
            tempFile.delete(); // Limpiar archivo temporal
            return new String(fileData, "UTF-8");

        } catch (Exception e) {
            log.error("Error en procesamiento por lotes: {}", e.getMessage(), e);

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

    // =============== MÉTODOS UTILITARIOS ===============

    private int estimarTamanoResultado(List<InfraccionesRepositoryImpl> repositories,
                                       ParametrosFiltrosDTO filtros, String nombreQuery) {
        ParametrosFiltrosDTO filtrosPrueba = filtros.toBuilder()
                .limite(10)
                .pagina(0)
                .build();

        int totalEstimado = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            try {
                List<Map<String, Object>> muestra = repo.ejecutarQueryConFiltros(nombreQuery, filtrosPrueba);

                if (muestra.size() >= 10) {
                    totalEstimado += 1000;
                } else if (muestra.size() > 0) {
                    totalEstimado += muestra.size() * 10;
                }

            } catch (Exception e) {
                log.warn("No se pudo estimar para provincia {}: {}", repo.getProvincia(), e.getMessage());
                totalEstimado += 500;
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
            List<String> provinciasNormalizadas = validator.normalizarProvincias(filtros.getBaseDatos());

            return provinciasNormalizadas.stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(provincia -> (InfraccionesRepositoryImpl) repositoryFactory.getRepository(provincia))
                    .collect(Collectors.toList());
        } else {
            return repositoryFactory.getAllRepositories().values().stream()
                    .map(repo -> (InfraccionesRepositoryImpl) repo)
                    .collect(Collectors.toList());
        }
    }


}