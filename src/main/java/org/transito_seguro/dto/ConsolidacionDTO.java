package org.transito_seguro.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsolidacionDTO {

    private List<String> camposAgrupacion;

    // Metodo utilitario
    public boolean esValido(){
        return camposAgrupacion != null && !camposAgrupacion.isEmpty();
    }


}
