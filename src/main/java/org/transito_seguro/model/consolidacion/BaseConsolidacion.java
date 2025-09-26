package org.transito_seguro.model.consolidacion;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class BaseConsolidacion {

    protected final List<String> camposAgrupacion;
    protected final List<String> camposNumericos;
    protected final List<String> camposTiempo;
    protected final List<String> camposUbicacion;
}
