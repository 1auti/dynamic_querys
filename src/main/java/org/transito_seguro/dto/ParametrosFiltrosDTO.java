package org.transito_seguro.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
public class ParametrosFiltrosDTO {

    // =================== FILTROS DE FECHA ===================
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date fechaInicio;

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date fechaFin;

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date fechaEspecifica;

    /**
     * Indica si usar fecha específica en lugar de rango
     */
    private Boolean usarFechaEspecifica;

    // =================== FILTROS DE UBICACIÓN ===================
    /**
     * Lista de bases de datos/provincias a consultar
     */
    @Size(max = 10, message = "Máximo 10 provincias por consulta")
    private List<String> baseDatos;

    /**
     * Lista de municipios específicos (opcional)
     */
    @Size(max = 50, message = "Máximo 50 municipios por consulta")
    private List<String> municipios;

    /**
     * Si es true, consulta todas las bases de datos disponibles
     */
    @Builder.Default
    private Boolean usarTodasLasBDS = true;

    // =================== FILTROS DE EQUIPOS ===================
    /**
     * Lista de series de equipos específicos
     */
    private List<String> seriesEquipos;

    /**
     * Tipos de dispositivos (1=Radar, 2=Cámara, etc.)
     */
    private List<Integer> tiposDispositivos;

    /**
     * Ubicaciones/lugares específicos de equipos
     */
    private List<String> lugares;

    // =================== FILTROS DE INFRACCIONES ===================
    /**
     * Tipos de infracciones específicas (1=Velocidad, 3=Luz Roja, 4=Senda Peatonal, etc.)
     */
    private List<Integer> tiposInfracciones;

    /**
     * Estados de infracciones
     */
    private List<Integer> estadosInfracciones;

    /**
     * Filtro por exportación a SACIT
     */
    private Boolean exportadoSacit;

    /**
     * Rango de montos - mínimo
     */
    private BigDecimal montoMinimo;

    /**
     * Rango de montos - máximo
     */
    private BigDecimal montoMaximo;

    // =================== FILTROS DE DOCUMENTOS ===================
    /**
     * Tipos de documentos (DNI, CUIT, etc.)
     */
    private List<String> tiposDocumentos;

    /**
     * Números de documentos específicos
     */
    private List<String> numerosDocumentos;

    /**
     * Filtrar solo personas jurídicas
     */
    private Boolean soloPersonasJuridicas;

    /**
     * Filtrar solo personas físicas
     */
    private Boolean soloPersonasFisicas;

    // =================== FILTROS DE DOMINIOS ===================
    /**
     * Dominios específicos a consultar
     */
    private List<String> dominios;

    /**
     * Marcas de vehículos
     */
    private List<String> marcas;

    /**
     * Modelos de vehículos
     */
    private List<String> modelos;

    /**
     * Tipos de vehículos
     */
    private List<String> tiposVehiculos;

    // =================== PARÁMETROS DE PAGINACIÓN ===================
    /**
     * Número de página (comenzando en 0)
     */

    private Integer pagina;


    private Integer tamanoPagina;

    /**
     * Límite máximo de registros a devolver
     */
    private Integer limiteMaximo;

    // =================== PARÁMETROS DE ORDENAMIENTO ===================
    /**
     * Campo por el cual ordenar
     */
    private String ordenarPor;

    /**
     * Dirección del ordenamiento (ASC/DESC)
     */
    @Builder.Default
    private String direccionOrdenamiento = "ASC";

    // =================== FILTROS ADICIONALES ===================
    /**
     * Filtro de texto libre para búsquedas
     */
    private String textoBusqueda;

    /**
     * Incluir registros eliminados lógicamente
     */
    @Builder.Default
    private Boolean incluirEliminados = false;

    /**
     * Filtrar por imágenes disponibles
     */
    private Boolean tieneImagenes;

    /**
     * Filtrar por email disponible
     */
    private Boolean tieneEmail;

    // =================== MÉTODOS DE UTILIDAD ===================

    /**
     * Verifica si se debe usar filtro de fecha específica
     */
    public boolean debeUsarFechaEspecifica() {
        return usarFechaEspecifica != null && usarFechaEspecifica && fechaEspecifica != null;
    }

    /**
     * Verifica si hay filtros de rango de fechas
     */
    public boolean tieneRangoFechas() {
        return fechaInicio != null || fechaFin != null;
    }

    /**
     * Verifica si debe consultar todas las bases de datos
     */
    public boolean debeConsultarTodasLasBDS() {
        return usarTodasLasBDS != null && usarTodasLasBDS;
    }

    /**
     * Verifica si hay filtros de ubicación específicos
     */
    public boolean tieneFiltrosUbicacion() {
        return (baseDatos != null && !baseDatos.isEmpty()) ||
                (municipios != null && !municipios.isEmpty());
    }

    /**
     * Verifica si hay filtros de equipos
     */
    public boolean tieneFiltrosEquipos() {
        return (seriesEquipos != null && !seriesEquipos.isEmpty()) ||
                (tiposDispositivos != null && !tiposDispositivos.isEmpty()) ||
                (lugares != null && !lugares.isEmpty());
    }

    /**
     * Verifica si hay filtros de infracciones
     */
    public boolean tieneFiltrosInfracciones() {
        return (tiposInfracciones != null && !tiposInfracciones.isEmpty()) ||
                (estadosInfracciones != null && !estadosInfracciones.isEmpty()) ||
                exportadoSacit != null ||
                montoMinimo != null ||
                montoMaximo != null;
    }

    /**
     * Calcula el offset para paginación
     */
    public int calcularOffset() {
        return pagina != null && tamanoPagina != null ? pagina * tamanoPagina : 0;
    }

    /**
     * Obtiene el tamaño de página seguro (con límite)
     */
    public int obtenerTamanoPaginaSeguro() {
        if (tamanoPagina == null) return 100;
        return Math.min(tamanoPagina, 1000); // Máximo 1000 registros por página
    }
}