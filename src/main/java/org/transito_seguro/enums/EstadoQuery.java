package org.transito_seguro.enums;

public enum EstadoQuery {
    PENDIENTE,      // Recién creada, no analizada
    ANALIZADA,      // Análisis completado exitosamente
    ERROR,          // Error en análisis
    REGISTRADA,     // Manualmente registrada por admin
    OBSOLETA        // Query ya no se usa
}
