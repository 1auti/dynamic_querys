package org.transito_seguro.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Pattern;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ConsultaQueryDTO {

    @Pattern(regexp = "csv|excel|json", message = "El formato debe ser csv, excel o json")
    private String formato;
    private ParametrosFiltrosDTO parametrosFiltros;

}
