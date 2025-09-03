package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.TaskInfo;
import org.transito_seguro.dto.TaskSummary;
import org.transito_seguro.enums.TaskStatus;
import org.transito_seguro.service.TaskService;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/tareas")
@CrossOrigin(origins = "*", maxAge = 3600)
public class TaskController {

    @Autowired
    private TaskService taskService;

    // Constantes para respuestas reutilizables
    private static final Map<String, String> ERROR_MESSAGES = new HashMap<>();
    static {
        ERROR_MESSAGES.put("CONSULTA_ERROR", "Error iniciando consulta");
        ERROR_MESSAGES.put("TAREA_NO_ENCONTRADA", "Tarea no encontrada");
        ERROR_MESSAGES.put("DESCARGA_ERROR", "Error descargando resultado");
        ERROR_MESSAGES.put("TAREA_NO_COMPLETADA", "La tarea no está completada");
        ERROR_MESSAGES.put("TAREA_NO_CANCELABLE", "La tarea no puede ser cancelada");
    }


    // =================== ENDPOINTS PRINCIPALES ===================

    /**
     * Inicia una consulta asíncrona de personas jurídicas
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<Map<String, Object>> iniciarConsultaPersonasJuridicas(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            log.info("Iniciando consulta asíncrona de personas jurídicas");

            TaskInfo taskInfo = taskService.iniciarConsultaAsincrona(consulta, "personas-juridicas");

            Map<String, Object> response = buildSuccessResponse(taskInfo);
            return ResponseEntity.accepted().body(response);

        } catch (Exception e) {
            log.error("Error iniciando consulta asíncrona", e);
            return ResponseEntity.badRequest().body(
                    buildErrorResponse("CONSULTA_ERROR", e.getMessage())
            );
        }
    }

    /**
     * Inicia cualquier tipo de consulta asíncrona
     */
    @PostMapping("/consulta/{tipoConsulta}")
    public ResponseEntity<Map<String, Object>> iniciarConsultaGenerica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        try {
            log.info("Iniciando consulta asíncrona tipo: {}", tipoConsulta);

            TaskInfo taskInfo = taskService.iniciarConsultaAsincrona(consulta, tipoConsulta);

            Map<String, Object> response = new LinkedHashMap<>(); // Mantiene orden de inserción
            response.put("taskId", taskInfo.getTaskId());
            response.put("status", taskInfo.getStatus());
            response.put("tipoConsulta", tipoConsulta);
            response.put("progresoUrl", buildUrl("progreso", taskInfo.getTaskId()));

            return ResponseEntity.accepted().body(response);

        } catch (Exception e) {
            log.error("Error iniciando consulta {}", tipoConsulta, e);
            Map<String, Object> detallesError = new HashMap<>();
            detallesError.put("tipoConsulta", tipoConsulta);
            return ResponseEntity.badRequest().body(
                    buildErrorResponse("CONSULTA_ERROR", e.getMessage(), detallesError)
            );
        }
    }

    // =================== ENDPOINTS DE MONITOREO ===================

    /**
     * Obtiene el progreso detallado de una tarea
     */
    @GetMapping("/{taskId}/progreso")
    public ResponseEntity<Map<String, Object>> obtenerProgreso(@PathVariable String taskId) {
        try {
            TaskInfo taskInfo = taskService.obtenerEstadoTarea(taskId);
            Map<String, Object> response = buildProgressResponse(taskInfo);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.warn("Tarea no encontrada: {}", taskId);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Obtiene información completa de una tarea
     */
    @GetMapping("/{taskId}/info")
    public ResponseEntity<TaskInfo> obtenerInfoCompleta(@PathVariable String taskId) {
        try {
            TaskInfo taskInfo = taskService.obtenerEstadoTarea(taskId);
            return ResponseEntity.ok(taskInfo);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Obtiene resumen de una tarea (respuesta ligera)
     */
    @GetMapping("/{taskId}/resumen")
    public ResponseEntity<TaskSummary> obtenerResumen(@PathVariable String taskId) {
        try {
            TaskInfo taskInfo = taskService.obtenerEstadoTarea(taskId);
            TaskSummary summary = TaskSummary.fromTaskInfo(taskInfo);
            return ResponseEntity.ok(summary);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // =================== ENDPOINTS DE DESCARGA ===================

    /**
     * Descarga el resultado de una tarea completada
     */
    @GetMapping("/{taskId}/descargar")
    public ResponseEntity<?> descargarResultado(@PathVariable String taskId, HttpServletResponse response) {
        try {
            TaskInfo taskInfo = taskService.obtenerEstadoTarea(taskId);

            if (taskInfo.getStatus() != TaskStatus.COMPLETADO) {
                Map<String, Object> errorDetails = new HashMap<>();
                errorDetails.put("status", taskInfo.getStatus().toString());
                return ResponseEntity.badRequest().body(
                        buildErrorResponse("TAREA_NO_COMPLETADA",
                                "Espere a que la tarea termine antes de descargar",
                                errorDetails)
                );
            }

            Object resultado = taskService.descargarResultado(taskId);
            String formato = taskInfo.getParametros().getFormato();

            return buildDownloadResponse(resultado, formato, taskId);

        } catch (Exception e) {
            log.error("Error descargando resultado de tarea {}", taskId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                    buildErrorResponse("DESCARGA_ERROR", e.getMessage())
            );
        }
    }

    // =================== ENDPOINTS DE CONTROL ===================

    /**
     * Cancela una tarea en progreso
     */
    @PostMapping("/{taskId}/cancelar")
    public ResponseEntity<Map<String, Object>> cancelarTarea(@PathVariable String taskId) {
        try {
            TaskInfo taskInfo = taskService.obtenerEstadoTarea(taskId);

            if (!taskInfo.puedeSerCancelada()) {
                Map<String, Object> errorDetails = new HashMap<>();
                errorDetails.put("status", taskInfo.getStatus().toString());
                return ResponseEntity.badRequest().body(
                        buildErrorResponse("TAREA_NO_CANCELABLE",
                                "Solo las tareas en progreso pueden ser canceladas",
                                errorDetails)
                );
            }


            taskService.cancelarTarea(taskId);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("mensaje", "Tarea cancelada exitosamente");
            response.put("taskId", taskId);
            response.put("timestamp", System.currentTimeMillis());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // =================== ENDPOINTS DE ADMINISTRACIÓN ===================

    /**
     * Lista todas las tareas (para administración)
     */
    @GetMapping("/listar")
    public ResponseEntity<Map<String, Object>> listarTareas(
            @RequestParam(required = false) TaskStatus status,
            @RequestParam(required = false, defaultValue = "50") int limite) {

        List<TaskInfo> todasLasTareas = taskService.obtenerTodasLasTareas();

        List<TaskSummary> summaries = todasLasTareas.stream()
                .filter(task -> status == null || task.getStatus() == status)
                .limit(limite)
                .map(TaskSummary::fromTaskInfo)
                .collect(Collectors.toList());

        // Respuesta estructurada con HashMap
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("total", summaries.size());
        response.put("filtro", status != null ? status.toString() : "TODOS");
        response.put("limite", limite);
        response.put("tareas", summaries);
        response.put("timestamp", System.currentTimeMillis());

        return ResponseEntity.ok(response);
    }

    /**
     * Obtiene estadísticas del servicio de tareas
     */
    @GetMapping("/estadisticas")
    public ResponseEntity<Map<String, Object>> obtenerEstadisticas() {
        Map<String, Object> stats = taskService.obtenerEstadisticas();

        // Enriquecer estadísticas con información adicional
        Map<String, Object> response = new LinkedHashMap<>(stats);
        response.put("timestamp", System.currentTimeMillis());
        response.put("servidor", "TaskController v1.0");

        return ResponseEntity.ok(response);
    }

    /**
     * Limpia tareas antiguas (endpoint de mantenimiento)
     */
    @PostMapping("/limpiar")
    public ResponseEntity<Map<String, Object>> limpiarTareasAntiguas() {
        try {
            Map<String, Object> resultado = taskService.limpiarTareasAntiguas();

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("mensaje", "Limpieza de tareas completada");
            response.put("detalles", resultado);
            response.put("timestamp", System.currentTimeMillis());

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                    buildErrorResponse("LIMPIEZA_ERROR", e.getMessage())
            );
        }
    }

    // =================== ENDPOINT DE AYUDA ===================

    /**
     * Información de ayuda sobre el uso de tareas asíncronas
     */
    @GetMapping("/ayuda")
    public ResponseEntity<Map<String, Object>> obtenerAyuda() {
        Map<String, Object> ayuda = buildHelpResponse();
        return ResponseEntity.ok(ayuda);
    }

    // =================== MÉTODOS HELPER PRIVADOS ===================

    /**
     * Construye respuesta de éxito estándar
     */
    private Map<String, Object> buildSuccessResponse(TaskInfo taskInfo) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("taskId", taskInfo.getTaskId());
        response.put("status", taskInfo.getStatus());
        response.put("mensaje", "Consulta iniciada. Use el taskId para monitorear progreso.");
        response.put("tiempoEstimadoMinutos", "5-10 minutos");
        response.put("urls", buildUrlsMap(taskInfo.getTaskId()));
        response.put("timestamp", System.currentTimeMillis());

        return response;
    }

    /**
     * Construye mapa de URLs para una tarea
     */
    private Map<String, String> buildUrlsMap(String taskId) {
        Map<String, String> urls = new LinkedHashMap<>();
        urls.put("progreso", buildUrl("progreso", taskId));
        urls.put("descargar", buildUrl("descargar", taskId));
        urls.put("cancelar", buildUrl("cancelar", taskId));
        urls.put("info", buildUrl("info", taskId));

        return urls;
    }

    /**
     * Construye URL para endpoint específico
     */
    private String buildUrl(String endpoint, String taskId) {
        return String.format("/api/tareas/%s/%s", taskId, endpoint);
    }

    /**
     * Construye respuesta de progreso optimizada
     */
    private Map<String, Object> buildProgressResponse(TaskInfo taskInfo) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("taskId", taskInfo.getTaskId());
        response.put("status", taskInfo.getStatus());
        response.put("porcentaje", taskInfo.getProgreso() != null ?
                taskInfo.getProgreso().getPorcentajeTotal() : 0.0);

        if (taskInfo.getProgreso() != null) {
            Map<String, Object> progreso = new LinkedHashMap<>();
            progreso.put("provinciaActual", taskInfo.getProgreso().getProvinciaActual());
            progreso.put("loteActual", taskInfo.getProgreso().getLoteActual());
            progreso.put("registrosProcesados", taskInfo.getProgreso().getRegistrosProcesados());
            progreso.put("mensaje", taskInfo.getProgreso().getMensaje());
            progreso.put("velocidad", taskInfo.getProgreso().getVelocidadProcesamiento());

            response.put("progreso", progreso);
        }

        response.put("duracionSegundos", taskInfo.getDuracionSegundos());
        response.put("estaTerminada", taskInfo.estaTerminada());
        response.put("timestamp", System.currentTimeMillis());

        if (taskInfo.getMensajeError() != null) {
            response.put("error", taskInfo.getMensajeError());
        }

        if (taskInfo.getStatus() == TaskStatus.COMPLETADO) {
            response.put("puedeDescargar", true);
            response.put("urlDescarga", buildUrl("descargar", taskInfo.getTaskId()));
        }

        return response;
    }

    /**
     * Construye respuesta de error estándar
     */
    private Map<String, Object> buildErrorResponse(String errorKey, String detalle) {
        return buildErrorResponse(errorKey, detalle, null);
    }

    private Map<String, Object> buildErrorResponse(String errorKey, String detalle, Map<String, Object> extras) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("error", ERROR_MESSAGES.getOrDefault(errorKey, "Error desconocido"));
        error.put("detalle", detalle);
        error.put("timestamp", System.currentTimeMillis());

        if (extras != null) {
            error.putAll(extras);
        }

        return error;
    }

    /**
     * Construye respuesta de descarga
     */
    private ResponseEntity<?> buildDownloadResponse(Object resultado, String formato, String taskId) {

        Map<String, MediaType> formatMap = new HashMap<>();
        formatMap.put("csv", MediaType.TEXT_PLAIN);
        formatMap.put("excel", MediaType.APPLICATION_OCTET_STREAM);
        formatMap.put("json", MediaType.APPLICATION_JSON);

        Map<String, String> extensionMap = new HashMap<>();
        extensionMap.put("csv", "csv");
        extensionMap.put("excel", "xlsx");
        extensionMap.put("json", "json");

        MediaType mediaType = formatMap.getOrDefault(formato.toLowerCase(), MediaType.APPLICATION_JSON);
        String extension = extensionMap.getOrDefault(formato.toLowerCase(), "json");
        String filename = String.format("infracciones_%s.%s", taskId, extension);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(mediaType)
                .body(resultado);
    }

    /**
     * Construye respuesta de ayuda estructurada
     */
    private Map<String, Object> buildHelpResponse() {
        Map<String, Object> ayuda = new LinkedHashMap<>();

        ayuda.put("descripcion", "API para ejecutar consultas de infracciones de forma asíncrona");

        // Flujo de trabajo
        Map<String, String> flujo = new LinkedHashMap<>();
        flujo.put("paso1", "POST /api/tareas/personas-juridicas - Iniciar consulta");
        flujo.put("paso2", "GET /api/tareas/{taskId}/progreso - Monitorear progreso");
        flujo.put("paso3", "GET /api/tareas/{taskId}/descargar - Descargar resultado");
        ayuda.put("flujoTrabajo", flujo);

        // Estados posibles
        Map<String, String> estados = new LinkedHashMap<>();
        estados.put("INICIADO", "Tarea creada y en cola");
        estados.put("PROCESANDO", "Tarea en ejecución");
        estados.put("COMPLETADO", "Tarea terminada exitosamente");
        estados.put("ERROR", "Tarea falló");
        estados.put("CANCELADO", "Tarea cancelada por usuario");
        ayuda.put("estados", estados);

        // Tiempos estimados
        Map<String, String> tiempos = new LinkedHashMap<>();
        tiempos.put("pequeña", "< 1000 registros: 1-2 minutos");
        tiempos.put("mediana", "1000-50000 registros: 3-8 minutos");
        tiempos.put("grande", "> 50000 registros: 10-30 minutos");
        ayuda.put("tiemposEstimados", tiempos);

        ayuda.put("formatos", Arrays.asList("json", "csv", "excel"));

        // Ejemplo de request
        Map<String, Object> ejemploRequest = new LinkedHashMap<>();
        ejemploRequest.put("formato", "excel");

        Map<String, Object> parametros = new LinkedHashMap<>();
        parametros.put("fechaInicio", "2024-01-01");
        parametros.put("fechaFin", "2024-12-31");
        parametros.put("baseDatos", Arrays.asList("Buenos Aires", "Córdoba"));

        ejemploRequest.put("parametrosFiltros", parametros);
        ayuda.put("ejemploRequest", ejemploRequest);

        ayuda.put("timestamp", System.currentTimeMillis());

        return ayuda;
    }
}