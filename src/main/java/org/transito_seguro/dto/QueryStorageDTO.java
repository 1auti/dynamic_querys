package org.transito_seguro.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryStorageDTO {

    @NotBlank(message = "El código es obligatorio")
    @Size(max = 100, message = "El código no puede exceder 100 caracteres")
    private String codigo;

    @NotBlank(message = "El nombre es obligatorio")
    @Size(max = 200, message = "El nombre no puede exceder 200 caracteres")
    private String nombre;

    private String descripcion;

    @NotBlank(message = "El SQL es obligatorio")
    private String sqlQuery;

    private String categoria;

    // Metadata de consolidación
    private Boolean esConsolidable = false;
    private List<String> camposAgrupacion;
    private List<String> camposNumericos;
    private List<String> camposUbicacion;
    private List<String> camposTiempo;

    // Configuración
    private Integer timeoutSegundos = 30;
    private Integer limiteMaximo = 50000;
    private List<String> tags;

    // Metadatos de creación
    private String creadoPor;
    private Boolean activa = true;

    // =============== VALIDACIONES ===============

    public boolean esValida() {
        return codigo != null && !codigo.trim().isEmpty() &&
                nombre != null && !nombre.trim().isEmpty() &&
                sqlQuery != null && !sqlQuery.trim().isEmpty() &&
                sqlQuery.toUpperCase().contains("SELECT");
    }

    public boolean deberiaSerConsolidable() {
        return esConsolidable && camposNumericos != null && !camposNumericos.isEmpty();
    }
}