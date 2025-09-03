package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.TaskInfo;
import org.transito_seguro.dto.TaskProgress;

import org.transito_seguro.component.FormatoConverter;
import org.transito_seguro.enums.TaskStatus;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class TaskService {

    @Autowired
    private BathProcessorService batchProcessorService;

    @Autowired
    private FormatoConverter formatoConverter;

    // Storage en memoria para tareas (en producción usar Redis/DB)
    private final Map<String, TaskInfo> taskStorage = new ConcurrentHashMap<>();
    private final Map<String, Object> resultStorage = new ConcurrentHashMap<>();

    /**
     * Inicia una consulta asíncrona
     */
    public TaskInfo iniciarConsultaAsincrona(ConsultaQueryDTO consulta, String tipoConsulta) {
        String taskId = generarTaskId();

        TaskInfo taskInfo = TaskInfo.builder()
                .taskId(taskId)
                .tipoConsulta(tipoConsulta)
                .parametros(consulta)
                .status(TaskStatus.INICIADO)
                .fechaInicio(LocalDateTime.now())
                .progreso(TaskProgress.builder()
                        .porcentajeTotal(0.0)
                        .registrosProcesados(0L)
                        .provinciaActual(null)
                        .loteActual(0)
                        .totalLotes(0)
                        .build())
                .build();

        taskStorage.put(taskId, taskInfo);

        log.info("Iniciando tarea asíncrona {}: {}", taskId, tipoConsulta);

        // Iniciar procesamiento asíncrono
        procesarConsultaAsync(taskInfo);

        return taskInfo;
    }

    /**
     * Obtiene el estado de una tarea
     */
    public TaskInfo obtenerEstadoTarea(String taskId) {
        TaskInfo taskInfo = taskStorage.get(taskId);
        if (taskInfo == null) {
            throw new RuntimeException("Tarea no encontrada: " + taskId);
        }
        return taskInfo;
    }

    /**
     * Descarga el resultado de una tarea completada
     */
    public Object descargarResultado(String taskId) {
        TaskInfo taskInfo = obtenerEstadoTarea(taskId);

        if (taskInfo.getStatus() != TaskStatus.COMPLETADO) {
            throw new RuntimeException("Tarea no completada: " + taskId);
        }

        Object resultado = resultStorage.get(taskId);
        if (resultado == null) {
            throw new RuntimeException("Resultado no disponible: " + taskId);
        }

        return resultado;
    }

    /**
     * Cancela una tarea en progreso
     */
    public void cancelarTarea(String taskId) {
        TaskInfo taskInfo = taskStorage.get(taskId);
        if (taskInfo != null && taskInfo.getStatus() == TaskStatus.PROCESANDO) {
            taskInfo.setStatus(TaskStatus.CANCELADO);
            taskInfo.setFechaFin(LocalDateTime.now());
            taskInfo.setMensajeError("Tarea cancelada por el usuario");

            log.info("Tarea cancelada: {}", taskId);
        }
    }

    /**
     * Limpia tareas completadas (ejecutar periódicamente)
     *
     * @return
     */
    public Map<String, Object> limpiarTareasAntiguas() {
        LocalDateTime hace24Horas = LocalDateTime.now().minusHours(24);

        taskStorage.entrySet().removeIf(entry -> {
            TaskInfo task = entry.getValue();
            boolean deberiaEliminar = task.getFechaFin() != null &&
                    task.getFechaFin().isBefore(hace24Horas);

            if (deberiaEliminar) {
                resultStorage.remove(entry.getKey());
                log.debug("Eliminada tarea antigua: {}", entry.getKey());
            }

            return deberiaEliminar;
        });
        return null;
    }

    /**
     * Procesamiento asíncrono principal
     */
    @Async("taskExecutor")
    public CompletableFuture<Void> procesarConsultaAsync(TaskInfo taskInfo) {
        String taskId = taskInfo.getTaskId();

        try {
            log.info("Iniciando procesamiento asíncrono para tarea: {}", taskId);

            // Actualizar estado
            taskInfo.setStatus(TaskStatus.PROCESANDO);
            taskInfo.setProgreso(taskInfo.getProgreso().toBuilder()
                    .mensaje("Iniciando consulta por lotes...")
                    .build());

            // Procesar por lotes
            List<Map<String, Object>> resultados = batchProcessorService.procesarPorLotes(
                    taskInfo.getParametros(),
                    progress -> actualizarProgreso(taskId, progress)
            );

            // Convertir resultado al formato solicitado
            String formato = taskInfo.getParametros().getFormato();
            if (formato == null) formato = "json";

            Object resultadoFormateado = formatoConverter.convertir(resultados, formato);

            // Guardar resultado
            resultStorage.put(taskId, resultadoFormateado);

            // Marcar como completado
            taskInfo.setStatus(TaskStatus.COMPLETADO);
            taskInfo.setFechaFin(LocalDateTime.now());
            taskInfo.setProgreso(taskInfo.getProgreso().toBuilder()
                    .porcentajeTotal(100.0)
                    .mensaje("Consulta completada exitosamente")
                    .build());

            log.info("Tarea completada exitosamente: {} - {} registros",
                    taskId, resultados.size());

        } catch (Exception e) {
            log.error("Error procesando tarea {}: {}", taskId, e.getMessage(), e);

            // Marcar como error
            taskInfo.setStatus(TaskStatus.ERROR);
            taskInfo.setFechaFin(LocalDateTime.now());
            taskInfo.setMensajeError("Error procesando consulta: " + e.getMessage());
            taskInfo.setProgreso(taskInfo.getProgreso().toBuilder()
                    .mensaje("Error: " + e.getMessage())
                    .build());
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Actualiza el progreso de una tarea
     */
    private void actualizarProgreso(String taskId, TaskProgress nuevoProgreso) {
        TaskInfo taskInfo = taskStorage.get(taskId);
        if (taskInfo != null && taskInfo.getStatus() == TaskStatus.PROCESANDO) {
            taskInfo.setProgreso(nuevoProgreso);
            log.debug("Progreso actualizado para tarea {}: {}%",
                    taskId, nuevoProgreso.getPorcentajeTotal());
        }
    }

    /**
     * Genera un ID único para la tarea
     */
    private String generarTaskId() {
        return "task-" + System.currentTimeMillis() + "-" +
                UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Obtiene todas las tareas (para administración)
     */
    public List<TaskInfo> obtenerTodasLasTareas() {
        return new ArrayList<>(taskStorage.values());
    }

    /**
     * Obtiene estadísticas del servicio
     */
    public Map<String, Object> obtenerEstadisticas() {
        Map<String, Object> stats = new HashMap<>();

        long tareasActivas = taskStorage.values().stream()
                .filter(task -> task.getStatus() == TaskStatus.PROCESANDO)
                .count();

        long tareasCompletadas = taskStorage.values().stream()
                .filter(task -> task.getStatus() == TaskStatus.COMPLETADO)
                .count();

        long tareasConError = taskStorage.values().stream()
                .filter(task -> task.getStatus() == TaskStatus.ERROR)
                .count();

        stats.put("tareasActivas", tareasActivas);
        stats.put("tareasCompletadas", tareasCompletadas);
        stats.put("tareasConError", tareasConError);
        stats.put("totalTareas", taskStorage.size());
        stats.put("totalResultados", resultStorage.size());

        return stats;
    }
}