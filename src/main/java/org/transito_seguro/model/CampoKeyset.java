package org.transito_seguro.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.transito_seguro.enums.TipoDatoKeyset;

@AllArgsConstructor
@Getter
public class CampoKeyset {

    private final String nombreCampo;
    private final String nombreParametro;
    private final TipoDatoKeyset tipoDato;
    private final int prioridad;

    public String getParametroJdbc(){
        return ":" + nombreParametro + "::" + tipoDato.getSqlType();
    }



    public static int getPrioridadTipo(TipoDatoKeyset tipo) {
        switch (tipo) {
            case TEXT: return 1;
            case DATE:
            case TIMESTAMP: return 2;
            case INTEGER: return 3;
            case BOOLEAN: return 4;
            default: return 5;
        }
    }

}
