package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ParametrosFiltrosDTO {

    private Date fechaInicio;
    private Date fechaFin;
    private Date fechaEspecifica;
    private List<String> baseDatos;

    private Boolean usarFechaEspecifica;
    private Boolean usarTodasLasBDS = true;
}
