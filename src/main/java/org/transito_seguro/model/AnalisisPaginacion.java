package org.transito_seguro.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.transito_seguro.enums.EstrategiaPaginacion;

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
