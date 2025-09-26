package org.transito_seguro.model.consolidacion.configuracion;

import lombok.Getter;
import lombok.Setter;

import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.consolidacion.BaseConsolidacion;

import java.util.List;


/**
 * Configuración de consolidación que define cómo procesar diferentes tipos de datos
 */

@Getter
@Setter
public class ConfiguracionConsolidacion extends BaseConsolidacion {

    private TipoConsolidacion tipo = TipoConsolidacion.AGREGACION;
    private boolean mantenerProvinciasOrigen = false;

    public ConfiguracionConsolidacion(List<String> camposAgrupacion, List<String> camposNumericos, List<String> camposTiempo, List<String> camposUbicacion) {
        super(camposAgrupacion, camposNumericos, camposTiempo, camposUbicacion);
    }
}
