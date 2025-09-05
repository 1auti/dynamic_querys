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

    private String tipoDocumento;
    private Integer limite, pagina;
    private Date fechaInicio, fechaFin, fechaEspecifica;
    private List<String> provincias, municipios, lugares, partido,
            baseDatos, patronesEquipos, tipoVehiculo, filtrarPorTipoEquipo, seriesEquiposExactas;
    private List<Integer> concesiones, tiposInfracciones, estadosInfracciones, tiposDispositivos;
    private Boolean exportadoSacit, tieneEmail, usarTodasLasBDS,
            incluirVLR, incluirSE, incluirTS, soloPersonasJuridicas;

    // NUEVOS CAMPOS PARA COINCIDIR CON TU JSON
    private Integer tamanoPagina;      // Nuevo campo
    private Integer limiteMaximo;      // Nuevo campo
    private Integer offset;            // Campo para offset explícito

    // Para casos edge
    private Map<String, Object> filtrosAdicionales;

    /**
     * Obtiene el límite efectivo considerando todas las fuentes
     */
    public Integer getLimiteEfectivo() {
        // Prioridad: limite > tamanoPagina > limiteMaximo > 5000 (default)
        if (limite != null && limite > 0) {
            return limite;
        }
        if (tamanoPagina != null && tamanoPagina > 0) {
            return tamanoPagina;
        }
        if (limiteMaximo != null && limiteMaximo > 0) {
            return Math.min(limiteMaximo, 10000); // Máximo seguro de 10000
        }
        return 5000; // Default aumentado
    }

    /**
     * Método original mantenido para compatibilidad
     */
    public Integer getLimiteMaximo() {
        return getLimiteEfectivo();
    }

    /**
     * Calcula el offset considerando diferentes fuentes
     */
    public Integer calcularOffset() {
        // Si viene offset explícito, usarlo
        if (offset != null && offset >= 0) {
            return offset;
        }

        // Calcular basado en página
        if (pagina != null && pagina >= 1) {
            return (pagina - 1) * getLimiteEfectivo();
        }

        return 0;
    }

    /**
     * Método para establecer límite dinámicamente
     */
    public ParametrosFiltrosDTO conLimite(int nuevoLimite) {
        this.limite = nuevoLimite;
        return this;
    }

    /**
     * Método para establecer tamaño de página dinámicamente
     */
    public ParametrosFiltrosDTO conTamanoPagina(int nuevoTamanoPagina) {
        this.tamanoPagina = nuevoTamanoPagina;
        return this;
    }

    /**
     * Método para establecer offset explícito
     */
    public ParametrosFiltrosDTO conOffset(int nuevoOffset) {
        this.offset = nuevoOffset;
        return this;
    }

    /**
     * Información de debug para logs
     */
    public String getInfoPaginacion() {
        return String.format("limite=%s, tamanoPagina=%s, limiteMaximo=%s, pagina=%s, offset=%s, efectivo=%d",
                limite, tamanoPagina, limiteMaximo, pagina, offset, getLimiteEfectivo());
    }

    /**
     * Valida que los parámetros de paginación sean consistentes
     */
    public boolean validarPaginacion() {
        int limiteEfectivo = getLimiteEfectivo();
        int offsetCalculado = calcularOffset();

        // Validaciones básicas
        if (limiteEfectivo <= 0 || limiteEfectivo > 50000) {
            return false;
        }

        if (offsetCalculado < 0) {
            return false;
        }

        return true;
    }

    /**
     * Normaliza los parámetros para evitar conflictos
     */
    public ParametrosFiltrosDTO normalizar() {
        return this.toBuilder()
                .limite(getLimiteEfectivo())    // Unificar en 'limite'
                .offset(calcularOffset())       // Calcular offset explícito
                .build();
    }
}