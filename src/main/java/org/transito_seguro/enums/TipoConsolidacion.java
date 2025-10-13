package org.transito_seguro.enums;

public enum TipoConsolidacion {
    AGREGACION,
    AGREGACION_STREMING,
    AGREGACION_ALTO_VOLUMEN,
    DEDUPLICACION,     // Eliminar duplicados por campos únicos
    JERARQUICA,        // Crear estructura anidada por ubicación/tiempo
    COMBINADA,          // Deduplicación + Agregación
    CRUDO, // La query retorno los daots crudos en memoria sin procesar
    CRUDO_STREAMING,
    FORZAR_AGREGACION;



    /**
     * Verifica si este tipo corresponde a una query agregada (con GROUP BY).
     * @return true si es AGREGACION, AGREGACION_STREAMING o AGREGACION_ALTO_VOLUMEN
     */
    public boolean esQueryAgregada() {
        return this == AGREGACION ||
                this == AGREGACION_STREMING ||
                this == AGREGACION_ALTO_VOLUMEN;
    }

    /**
     * Verifica si este tipo corresponde a una query cruda (sin GROUP BY).
     * @return true si es CRUDO, CRUDO_STREAMING o FORZAR_AGREGACION
     */
    public boolean esQueryCruda() {
        return this == CRUDO ||
                this == CRUDO_STREAMING ||
                this == FORZAR_AGREGACION;
    }

    /**
     * Verifica si este tipo requiere procesamiento con streaming.
     * @return true si el tipo incluye streaming en su estrategia
     */
    public boolean requiereStreaming() {
        return this == AGREGACION_STREMING ||
                this == AGREGACION_ALTO_VOLUMEN ||
                this == CRUDO_STREAMING;
    }

    /**
     * Obtiene una descripción legible del tipo.
     * @return Descripción para logs o UI
     */
    public String getDescripcion() {
        switch (this) {
            case AGREGACION:
                return "Agregación en memoria (bajo volumen)";
            case AGREGACION_STREMING:
                return "Agregación con streaming (volumen medio)";
            case AGREGACION_ALTO_VOLUMEN:
                return "Agregación con streaming optimizado (alto volumen)";
            case CRUDO:
                return "Procesamiento crudo en memoria (bajo volumen)";
            case CRUDO_STREAMING:
                return "Procesamiento crudo con streaming (volumen medio-alto)";
            case FORZAR_AGREGACION:
                return "Forzar agregación automática (volumen crítico)";
            case DEDUPLICACION:
                return "Deduplicación de registros";
            case JERARQUICA:
                return "Consolidación jerárquica multinivel";
            case COMBINADA:
                return "Deduplicación + Agregación combinada";
            default:
                return "Tipo desconocido";
        }
    }
}
