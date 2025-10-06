package org.transito_seguro.model.consolidacion.analisis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.List;

@Getter
@AllArgsConstructor
public class AnalisisPaginacion {

    private final EstrategiaPaginacion estrategiaPaginacion;
    private final boolean tieneIdInfracciones;
    private final List<CampoKeyset> camposDisponibles;
    private final String razon;

    public boolean requierePaginacion(){
        return estrategiaPaginacion != EstrategiaPaginacion.SIN_PAGINACION;
    }
}
