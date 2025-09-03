package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.transito_seguro.enums.TaskStatus;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class TaskSummary {

    private String taskId;
    private String tipoConsulta;
    private TaskStatus status;
    private Double porcentajeProgreso;
    private String mensajeProgreso;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime fechaInicio;

    private Long duracionSegundos;
    private String urlDescarga;
    private String mensajeError;

    /**
     * Crea un resumen desde TaskInfo completo
     */
    public static TaskSummary fromTaskInfo(TaskInfo taskInfo) {
        return TaskSummary.builder()
                .taskId(taskInfo.getTaskId())
                .tipoConsulta(taskInfo.getTipoConsulta())
                .status(taskInfo.getStatus())
                .porcentajeProgreso(taskInfo.getProgreso() != null ?
                        taskInfo.getProgreso().getPorcentajeTotal() : 0.0)
                .mensajeProgreso(taskInfo.getProgreso() != null ?
                        taskInfo.getProgreso().getMensaje() : null)
                .fechaInicio(taskInfo.getFechaInicio())
                .duracionSegundos(taskInfo.getDuracionSegundos())
                .urlDescarga(taskInfo.getUrlDescarga())
                .mensajeError(taskInfo.getMensajeError())
                .build();
    }
}