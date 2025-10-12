package org.transito_seguro.enums;

import lombok.Getter;

@Getter

public enum PeriodoTemporal {
    DIA("dia"),
    MES("mes"),
    ANIO("anio");

    private final String descripcion;

    PeriodoTemporal(String descripcion){
        this.descripcion = descripcion;
    }
}
