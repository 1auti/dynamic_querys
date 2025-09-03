package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.transito_seguro.dto.ConsultaQueryDTO;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.transito_seguro.enums.TaskStatus;

import java.time.LocalDateTime;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class TaskInfo {

    /**
     * ID único de la tarea
     */
    private String taskId;

    /**
     * Tipo de consulta ejecutada
     */
    private String tipoConsulta;

    /**
     * Estado actual de la tarea
     */
    private TaskStatus status;

    /**
     * Parámetros originales de la consulta
     */
    private ConsultaQueryDTO parametros;

    /**
     * Información de progreso
     */
    private TaskProgress progreso;

    /**
     * Fecha y hora de inicio
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime fechaInicio;

    /**
     * Fecha y hora de finalización
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime fechaFin;

    /**
     * Mensaje de error (si aplica)
     */
    private String mensajeError;

    /**
     * Metadata adicional
     */
    private Map<String, Object> metadata;

    /**
     * URL para descargar resultado (cuando esté disponible)
     */
    private String urlDescarga;

    /**
     * Tamaño estimado del resultado
     */
    private String tamanoResultado;

    /**
     * Formato del resultado
     */
    private String formatoResultado;

    /**
     * Calcula la duración total de la tarea
     */
    public Long getDuracionSegundos() {
        if (fechaInicio == null) return null;

        LocalDateTime fin = fechaFin != null ? fechaFin : LocalDateTime.now();
        return java.time.Duration.between(fechaInicio, fin).getSeconds();
    }

    /**
     * Verifica si la tarea está en un estado final
     */
    public boolean estaTerminada() {
        return status == TaskStatus.COMPLETADO ||
                status == TaskStatus.ERROR ||
                status == TaskStatus.CANCELADO;
    }

    /**
     * Verifica si la tarea puede ser cancelada
     */
    public boolean puedeSerCancelada() {
        return status == TaskStatus.INICIADO ||
                status == TaskStatus.PROCESANDO;
    }
}
