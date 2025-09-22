package org.transito_seguro.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryRegistrationDTO {

    @NotBlank(message = "El nombre de la query es obligatorio")
    private String nombreQuery;

    private String descripcion;

    private String categoria; // INFRACCIONES, VEHICULOS, REPORTES, etc.

    @NotNull(message = "Los campos de agrupación son obligatorios")
    private List<String> camposAgrupacion;

    private List<String> camposNumericos;

    private List<String> camposUbicacion;

    private List<String> camposTiempo;

    private Boolean forzarConsolidable = false; // Forzar como consolidable aunque no tenga campos numéricos

    // =============== VALIDACIONES ADICIONALES ===============

    /**
     * Valida que los campos de agrupación no estén vacíos
     */
    public boolean tieneCamposAgrupacion() {
        return camposAgrupacion != null && !camposAgrupacion.isEmpty();
    }

    /**
     * Valida que tenga campos numéricos (necesarios para consolidación)
     */
    public boolean tieneCamposNumericos() {
        return camposNumericos != null && !camposNumericos.isEmpty();
    }

    /**
     * Determina si debería ser consolidable
     */
    public boolean deberiaSerConsolidable() {
        return forzarConsolidable || (tieneCamposAgrupacion() && tieneCamposNumericos());
    }
}