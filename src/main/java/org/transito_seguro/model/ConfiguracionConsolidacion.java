package org.transito_seguro.model;

import lombok.Getter;
import lombok.Setter;
import org.transito_seguro.service.ConsolidacionService;

import java.util.ArrayList;
import java.util.List;


/**
 * Configuración de consolidación que define cómo procesar diferentes tipos de datos
 */

@Getter
@Setter
public class ConfiguracionConsolidacion {

    private List<String> camposAgrupacion = new ArrayList<>();
    private List<String> camposNumericos = new ArrayList<>();
    private List<String> camposUnicos = new ArrayList<>(); // Para deduplicación
    private List<String> camposTiempo = new ArrayList<>();
    private List<String> camposUbicacion = new ArrayList<>();
    private TipoConsolidacion tipo = TipoConsolidacion.AGREGACION;
    private boolean mantenerProvinciasOrigen = false;

}
