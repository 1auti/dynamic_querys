package org.transito_seguro.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * Metadata de un filtro disponible en una query SQL.
 *
 * Esta clase representa la información necesaria para que la interfaz gráfica
 * pueda construir controles de filtrado dinámicos basados en los filtros
 * detectados en cada query almacenada.
 *
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FiltroMetadata {

    /**
     * Tipo de filtro detectado.
     * Ejemplos: "DATE_RANGE", "ARRAY_INTEGER", "BOOLEAN", "SINGLE_VALUE"
     */
    private String tipoFiltro;

    /**
     * Campo SQL al que aplica el filtro.
     * Ejemplo: "i.fecha_infraccion", "i.id_estado"
     */
    private String campoSQL;

    /**
     * Nombre descriptivo del filtro para mostrar en la UI.
     * Ejemplo: "Fecha de Infracción", "Estados", "Tipo de Infracción"
     */
    private String etiqueta;

    /**
     * Nombres de los parámetros que acepta este filtro.
     *
     * Ejemplos:
     * - Fecha: ["fechaEspecifica", "fechaInicio", "fechaFin"]
     * - Estado: ["estadosInfracciones"]
     * - Boolean: ["exportadoSacit"]
     */
    private List<String> parametros;

    /**
     * Tipo de dato que acepta el filtro.
     * Ejemplos: "DATE", "INTEGER", "BOOLEAN", "STRING"
     */
    private String tipoDato;

    /**
     * Indica si el filtro acepta múltiples valores.
     * Ejemplo: true para estados (array), false para fecha específica
     */
    private boolean multiple;

    /**
     * Indica si el filtro es obligatorio o opcional.
     * Por defecto todos son opcionales (nullable).
     */
    @Builder.Default
    private boolean obligatorio = false;

    /**
     * Opciones predefinidas para el filtro (si aplica).
     * Usado para filtros con valores conocidos (ej: estados, tipos).
     *
     * Formato: [{"valor": 1, "etiqueta": "Pendiente"}, ...]
     */
    private List<OpcionFiltro> opciones;

    /**
     * Clase interna para representar opciones de un filtro.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OpcionFiltro {
        /**
         * Valor real del filtro (ID, código, etc.)
         */
        private Object valor;

        /**
         * Etiqueta descriptiva para mostrar en la UI
         */
        private String etiqueta;

        /**
         * Color o icono asociado (opcional, para UI)
         */
        private String metadataUI;
    }
}