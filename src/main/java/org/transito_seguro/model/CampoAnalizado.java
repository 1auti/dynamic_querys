package org.transito_seguro.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.transito_seguro.enums.TipoCampo;

@Getter
@Setter
@AllArgsConstructor
public class CampoAnalizado {
    public String expresionOriginal;
    public String expresionLimpia;
    public String nombreFinal;
    public TipoCampo tipo;
    public boolean esAgregacion;
    public boolean esCalculado;
}
