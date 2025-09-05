package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class ParametrosFiltrosDTO {

    private Integer limite,pagina;
    private Date fechaInicio, fechaFin,fechaEspecifica;
    private List<String> provincias, municipios,lugares,partido,
            baseDatos,patronesEquipos,tipoVehiculo,filtrarPorTipoEquipo,seriesEquiposExactas;
    private List<Integer> concesiones, tiposInfracciones, estadosInfracciones,tiposDispositivos;
    private Boolean exportadoSacit, tieneEmail,usarTodasLasBDS,
    incluirVLR,incluirSE,incluirTS;

    // Para casos edge
    private Map<String, Object> filtrosAdicionales;

    public Integer getLimiteMaximo() {
        return limite != null ? limite : 50; // por defecto 50 registros
    }

    public Integer calcularOffset() {
        if (pagina == null || pagina < 1) {
            return 0;
        }
        return (pagina - 1) * getLimiteMaximo();
    }
}