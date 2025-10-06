package org.transito_seguro.enums;

import lombok.Getter;

@Getter
public enum TipoDatoKeyset {

    INTEGER("INTEGER"),
    TEXT("TEXT"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    BOOLEAN("BOOLEAN");

    private final String sqlType;

    TipoDatoKeyset(String sqlType){
        this.sqlType = sqlType;
    }

    public static TipoDatoKeyset detectarTipoDato(String nombreCampo){
        String lower = nombreCampo.toLowerCase();

        if(lower.contains("id") || lower.contains("numero")){
            return INTEGER;
        }else if (lower.contains("fecha")){
            return DATE;
        }else if(lower.contains("is")){
            return BOOLEAN;
        }else{
            return TEXT;
        }
    }
}
