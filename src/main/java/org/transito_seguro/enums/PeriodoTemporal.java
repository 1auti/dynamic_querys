package org.transito_seguro.enums;

public enum PeriodoTemporal {
    DIA("dia"),
    MES("mes"),
    ANIO("anio");

    private final String descripcion;

    PeriodoTemporal(String descripcion){
        this.descripcion = descripcion;
    }


    public String getDescripcion(){
        return descripcion;
    }
}
