package org.transito_seguro.enums;

/**
 * Enum que define los tipos de consultas disponibles y sus archivos SQL correspondientes.
 * Sincronizado con los archivos SQL reales del sistema.
 */
public enum Consultas {

    // Consultas principales
    PERSONAS_JURIDICAS("consultar_personas_juridicas.sql", "Consulta de personas jurídicas"),
    INFRACCIONES_GENERAL("reporte_infracciones_general.sql", "Reporte general de infracciones"),
    INFRACCIONES_DETALLADO("reporte_infracciones_detallado.sql", "Reporte detallado de infracciones"),

    // Reportes por equipos
    INFRACCIONES_POR_EQUIPOS("reporte_infracciones_por_equipos.sql", "Reporte de infracciones por equipos"),
    RADAR_FIJO_POR_EQUIPO("reporte_radar_fijo_por_equipo.sql", "Reporte de radar fijo por equipo"),
    SEMAFORO_POR_EQUIPO("reporte_semaforo_por_equipo.sql", "Reporte de semáforos por equipo"),

    // Reportes especializados
    VEHICULOS_POR_MUNICIPIO("reporte_vehiculos_por_municipio.sql", "Reporte de vehículos por municipio"),
    SIN_EMAIL_POR_MUNICIPIO("reporte_sin_email_por_municipio.sql", "Reporte de infracciones sin email"),
    VERIFICAR_IMAGENES_RADAR("verificar_imagenes_subidas_radar_concesion.sql", "Verificación de imágenes de radar");

    private final String archivoQuery;
    private final String descripcion;

    Consultas(String archivoQuery, String descripcion) {
        this.archivoQuery = archivoQuery;
        this.descripcion = descripcion;
    }

    /**
     * Obtiene el nombre del archivo SQL asociado a la consulta
     */
    public String getArchivoQuery() {
        return archivoQuery;
    }

    /**
     * Obtiene la descripción de la consulta
     */
    public String getDescripcion() {
        return descripcion;
    }

    /**
     * Busca una consulta por su archivo SQL
     */
    public static Consultas findByArchivo(String archivo) {
        for (Consultas consulta : values()) {
            if (consulta.getArchivoQuery().equals(archivo)) {
                return consulta;
            }
        }
        throw new IllegalArgumentException("No se encontró consulta para archivo: " + archivo);
    }

    /**
     * Busca una consulta por su nombre (case-insensitive)
     */
    public static Consultas findByName(String nombre) {
        try {
            return valueOf(nombre.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Consulta no válida: " + nombre +
                    ". Consultas válidas: " + java.util.Arrays.toString(values()));
        }
    }

    /**
     * Verifica si existe una consulta con el archivo especificado
     */
    public static boolean existeArchivo(String archivo) {
        try {
            findByArchivo(archivo);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Obtiene todos los archivos SQL disponibles
     */
    public static java.util.List<String> getAllArchivos() {
        return java.util.Arrays.stream(values())
                .map(Consultas::getArchivoQuery)
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Obtiene información completa de todas las consultas
     */
    public static java.util.Map<String, String> getMapeoCompleto() {
        return java.util.Arrays.stream(values())
                .collect(java.util.stream.Collectors.toMap(
                        c -> c.name().toLowerCase().replace("_", "-"),
                        Consultas::getArchivoQuery
                ));
    }
}