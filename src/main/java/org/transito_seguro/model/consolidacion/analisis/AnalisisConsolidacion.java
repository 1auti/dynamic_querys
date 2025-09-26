package org.transito_seguro.model.consolidacion.analisis;

import lombok.Getter;
import lombok.Setter;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.model.consolidacion.BaseConsolidacion;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class AnalisisConsolidacion extends BaseConsolidacion {

    private final Map<String, TipoCampo> tipoPorCampo;
    private final boolean esConsolidado; // Si la query puede ser consolidada

    public AnalisisConsolidacion(List<String> camposAgrupacion, List<String> camposNumericos, List<String> camposTiempo, List<String> camposUbicacion, Map<String, TipoCampo> tipoPorCampo, boolean esConsolidado) {
        super(camposAgrupacion, camposNumericos, camposTiempo, camposUbicacion);
        this.tipoPorCampo = tipoPorCampo;
        this.esConsolidado = esConsolidado;
    }


    public  static  AnalisisConsolidacion crearAnalisisVacio() {
        return new AnalisisConsolidacion(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                false
        );
    }

}
