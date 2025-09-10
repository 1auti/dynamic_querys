package org.transito_seguro.wrapper;

import lombok.Builder;
import lombok.Data;
import java.util.List;
import java.util.Map;

/**
 * Wrapper para respuestas con paginación por cursor
 */
@Data
@Builder
public class CursorResponseWrapper<T> {

    /**
     * Los datos de la página actual
     */
    private List<T> datos;

    /**
     * Información de paginación por cursor
     */
    private CursorPageInfo pageInfo;

    /**
     * Metadatos adicionales
     */
    private Map<String, Object> metadata;

    /**
     * Total de elementos si está disponible (opcional)
     */
    private Long totalCount;

    @Data
    @Builder
    public static class CursorPageInfo {

        /**
         * Cursor para la siguiente página (null si no hay más)
         */
        private String nextCursor;

        /**
         * Cursor para la página anterior (null si es la primera)
         */
        private String prevCursor;

        /**
         * Si hay más páginas hacia adelante
         */
        private boolean hasNext;

        /**
         * Si hay páginas anteriores
         */
        private boolean hasPrev;

        /**
         * Tamaño de la página solicitada
         */
        private int pageSize;

        /**
         * Cantidad de elementos en esta página
         */
        private int currentPageSize;

        /**
         * Tipo de cursor usado
         */
        private String cursorType;

        /**
         * Información adicional sobre el cursor
         */
        private Map<String, Object> cursorInfo;
    }

    /**
     * Factory method para crear respuesta vacía
     */
    public static <T> CursorResponseWrapper<T> empty(int pageSize, String cursorType) {
        return CursorResponseWrapper.<T>builder()
                .datos(java.util.Collections.emptyList())
                .pageInfo(CursorPageInfo.builder()
                        .hasNext(false)
                        .hasPrev(false)
                        .pageSize(pageSize)
                        .currentPageSize(0)
                        .cursorType(cursorType)
                        .build())
                .totalCount(0L)
                .build();
    }

    /**
     * Factory method para crear respuesta desde lista de resultados
     */
    public static <T> CursorResponseWrapper<T> fromResults(
            List<T> resultados,
            int pageSize,
            String tipoCursor,
            String cursorActual,
            boolean hayAnterior) {

        // Determinar si hay más páginas
        boolean hasNext = resultados.size() > pageSize;
        List<T> datosFinales = hasNext ? resultados.subList(0, pageSize) : resultados;

        // Generar cursors para navegación
        String nextCursor = null;
        String prevCursor = cursorActual; // El cursor actual se convierte en prevCursor

        if (hasNext && !datosFinales.isEmpty()) {
            // Generar cursor basado en el último elemento
            T ultimoElemento = datosFinales.get(datosFinales.size() - 1);
            nextCursor = extraerCursorDeElemento(ultimoElemento, tipoCursor);
        }

        return CursorResponseWrapper.<T>builder()
                .datos(datosFinales)
                .pageInfo(CursorPageInfo.builder()
                        .nextCursor(nextCursor)
                        .prevCursor(hayAnterior ? prevCursor : null)
                        .hasNext(hasNext)
                        .hasPrev(hayAnterior)
                        .pageSize(pageSize)
                        .currentPageSize(datosFinales.size())
                        .cursorType(tipoCursor)
                        .build())
                .build();
    }

    /**
     * Extrae cursor del último elemento según el tipo
     */
    @SuppressWarnings("unchecked")
    private static String extraerCursorDeElemento(Object elemento, String tipoCursor) {
        if (elemento instanceof Map) {
            Map<String, Object> mapa = (Map<String, Object>) elemento;

            switch (tipoCursor) {
                case "id":
                    Object id = mapa.get("id");
                    return id != null ? String.valueOf(id) : null;

                case "fecha":
                    Object fecha = mapa.get("fecha_alta");
                    if (fecha == null) fecha = mapa.get("fecha");
                    return fecha != null ? String.valueOf(fecha) : null;

                case "fecha_id":
                    Object fechaFI = mapa.get("fecha_alta");
                    Object idFI = mapa.get("id");
                    if (fechaFI == null) fechaFI = mapa.get("fecha");
                    return (fechaFI != null && idFI != null) ?
                            fechaFI + "|" + idFI : null;

                default:
                    // Para cursor custom, intentar usar ID como fallback
                    Object fallback = mapa.get("id");
                    return fallback != null ? String.valueOf(fallback) : null;
            }
        }

        return null;
    }
}