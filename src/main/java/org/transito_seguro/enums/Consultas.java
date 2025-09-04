package org.transito_seguro.enums;

public enum Consultas {
    INFRACCIONES_POR_EQUIPO("reporte_infracciones_por_equipos.sql"),
    PERSONAS_JURUDICAS("consultar_personas_juridicas.sql"),
    INFRACCIONES_DETALLADO("reporte_infracciones_detallado.sql"),
    INFRACCIONES_GENERAL("reporte_infracciones_general.sql"),
    RADAR_FIJO_POR_EQUIPO("reporte_infracciones_por_equipo.sql"),
    SEMAFORO_POR_EQUIPO("reporte_semaforos_por_equipo.sql"),
    VEHICULOS_POR_MUNICIPIO("reporte_vehiculos_por_municipio.sql"),
    SIN_EMAIL_POR_MUNICIPIO("reporte_sin_email_por_municipio.sql"),
    VERIFICAR_IMAGENES_RADAR("verificar_imagenes_subidas_radar_concesion.sql");

    private final String archivoQuery;


    Consultas(String archivoQuery) {
        this.archivoQuery = archivoQuery;
    }

    public String getArchivoQuery(){
        return archivoQuery;
    }
}
