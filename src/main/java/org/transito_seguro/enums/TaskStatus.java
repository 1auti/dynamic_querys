package org.transito_seguro.enums;

public enum TaskStatus {
    INICIADO("La tarea ha sido creada y está en cola"),
    PROCESANDO("La tarea está siendo procesada"),
    COMPLETADO("La tarea se completó exitosamente"),
    ERROR("La tarea falló con error"),
    CANCELADO("La tarea fue cancelada por el usuario");

    private final String descripcion;

    TaskStatus(String descripcion) {
        this.descripcion = descripcion;
    }

    public String getDescripcion() {
        return descripcion;
    }
}
