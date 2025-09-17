package org.transito_seguro.model;

public enum TipoConsolidacion {
    AGREGACION,        // Sumar valores numéricos
    DEDUPLICACION,     // Eliminar duplicados por campos únicos
    JERARQUICA,        // Crear estructura anidada por ubicación/tiempo
    COMBINADA          // Deduplicación + Agregación
}
