package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
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
            incluirVLR, incluirSE, incluirTS, soloPersonasJuridicas,consolidado;


    private List<String> consolidacion;

    private Integer tamanoPagina;
    private Integer limiteMaximo;
    private Integer offset;

    // Para casos edge
    private Map<String, Object> filtrosAdicionales;

    /**
     * Obtiene el límite efectivo considerando todas las fuentes
     */
    /**
     * VERSIÓN CORREGIDA - Lógica coherente
     */
    public Integer getLimiteEfectivo() {
        // Si se usa todas las BDS y NO hay límite específico = SIN LÍMITE
        if (Boolean.TRUE.equals(usarTodasLasBDS) && limite == null && tamanoPagina == null) {
            return Integer.MAX_VALUE; // Prácticamente sin límite
        }

        // Prioridad normal: límite explícito del usuario
        if (limite != null && limite > 0) {
            return limite;
        }

        // Segundo: tamaño de página
        if (tamanoPagina != null && tamanoPagina > 0) {
            return tamanoPagina;
        }

        // Tercero: límite máximo configurado
        if (limiteMaximo != null && limiteMaximo > 0) {
            return Math.min(limiteMaximo, 50000); // Aumentado a 50K
        }

        // Default: más alto si usa todas las BDS, normal si no
        return Boolean.TRUE.equals(usarTodasLasBDS) ? 50000 : 5000;
    }

    /**
     * Método original mantenido para compatibilidad
     */
    public Integer getLimiteMaximo() {
        return getLimiteEfectivo();
    }

    public boolean esConsolidado() {
        return consolidado != null && consolidado &&
                consolidacion != null && !consolidacion.isEmpty();
    }

    public List<String> getConsolidacionSeguro() {
        return consolidacion != null ? consolidacion : new ArrayList<>();
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

   public static boolean tieneParametrosFecha(ParametrosFiltrosDTO filtros) {
        return filtros.getFechaInicio() != null ||
                filtros.getFechaFin() != null ||
                filtros.getFechaEspecifica() != null;
    }

    public static boolean tieneParametrosUbicacion(ParametrosFiltrosDTO filtros) {
        return (filtros.getProvincias() != null && !filtros.getProvincias().isEmpty()) ||
                (filtros.getMunicipios() != null && !filtros.getMunicipios().isEmpty());
    }

    public static boolean tieneParametrosEstado(ParametrosFiltrosDTO filtros) {
        return filtros.getExportadoSacit() != null ||
                filtros.getTieneEmail() != null ||
                (filtros.getEstadosInfracciones() != null && !filtros.getEstadosInfracciones().isEmpty());
    }

    public static boolean esParametroFechaBasico(String parametro) {
        String param = parametro.toLowerCase();
        return param.equals("fecha") ||
                param.equals("fechaespecifica") ||
                param.equals("fechainicio") ||
                param.equals("fechafin") ||
                param.equals("fecha_alta") ||
                param.equals("fecha_reporte");
    }
}