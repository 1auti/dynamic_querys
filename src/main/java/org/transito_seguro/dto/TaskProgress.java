package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
public class TaskProgress {

    /**
     * Porcentaje total de progreso (0.0 - 100.0)
     */
    private Double porcentajeTotal;

    /**
     * Provincia actualmente siendo procesada
     */
    private String provinciaActual;

    /**
     * Número del lote actual
     */
    private Integer loteActual;

    /**
     * Total de lotes estimados
     */
    private Integer totalLotes;

    /**
     * Total de registros procesados hasta el momento
     */
    private Long registrosProcesados;

    /**
     * Registros estimados totales
     */
    private Long registrosEstimados;

    /**
     * Mensaje descriptivo del progreso actual
     */
    private String mensaje;

    /**
     * Tiempo estimado restante en segundos
     */
    private Long tiempoEstimadoRestante;

    /**
     * Velocidad de procesamiento (registros/segundo)
     */
    private Double velocidadProcesamiento;

    /**
     * Provincias procesadas completamente
     */
    private java.util.List<String> provinciasCompletadas;

    /**
     * Provincias con error
     */
    private java.util.List<String> provinciasConError;
}