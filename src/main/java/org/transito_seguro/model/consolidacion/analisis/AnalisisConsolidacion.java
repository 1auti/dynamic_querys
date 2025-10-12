package org.transito_seguro.model.consolidacion.analisis;

import lombok.Data;
import lombok.AllArgsConstructor;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.enums.TipoConsolidacion;

import java.util.*;

/**
 * Resultado del análisis de una query SQL para consolidación.
 * Contiene toda la información necesaria para decidir la estrategia de procesamiento.
 */
@Data
@AllArgsConstructor
public class AnalisisConsolidacion {
    
    // ========== CAMPOS DE AGRUPACIÓN ==========
    
    /**
     * Campos que se usarán para agrupar (provincia, fecha, tipo, etc.)
     * Ejemplo: ["provincia", "fecha", "tipo_infraccion"]
     */
    private List<String> camposAgrupacion;
    
    /**
     * Campos numéricos que se agregarán (monto, cantidad, etc.)
     * Ejemplo: ["monto_total", "cantidad_infracciones"]
     */
    private List<String> camposNumericos;
    
    /**
     * Campos de tiempo (fecha, año, mes, etc.)
     * Ejemplo: ["fecha", "anio", "mes"]
     */
    private List<String> camposTiempo;
    
    /**
     * Campos de ubicación (provincia, municipio, lugar, etc.)
     * Ejemplo: ["provincia", "municipio"]
     */
    private List<String> camposUbicacion;
    
    /**
     * Mapa con el tipo de cada campo
     * Key: nombre del campo, Value: tipo (UBICACION, NUMERICO_SUMA, etc.)
     */
    private Map<String, TipoCampo> tipoPorCampo;
    
    // ========== ANÁLISIS DE CONSOLIDACIÓN ==========
    
    /**
     * Indica si la query puede ser consolidada.
     * Una query es consolidable si tiene campos numéricos y campos de agrupación.
     */
    private boolean esConsolidable;
    
    /**
     * Tipo de consolidación que requiere esta query.
     * - AGREGADA: Query con GROUP BY, retorna pocos registros (segura para memoria)
     * - CRUDA: Query sin GROUP BY, retorna muchos registros (necesita streaming)
     * - null: Query no es consolidable
     */
    private TipoConsolidacion tipoConsolidacion;
    
    /**
     * Estimación de cuántos registros retornará la query.
     * Se calcula basándose en:
     * - Si tiene GROUP BY: cardinalidad de campos de agrupación
     * - Si no tiene GROUP BY: se marca como null (necesita COUNT(*) dinámico)
     */
    private Integer registrosEstimados;
    
    /**
     * Nivel de confianza de la estimación (0.0 a 1.0)
     * - 1.0: Estimación muy confiable (GROUP BY con campos conocidos)
     * - 0.5: Estimación moderada (GROUP BY con campos desconocidos)
     * - 0.0: No se puede estimar (sin GROUP BY)
     */
    private Double confianzaEstimacion;
    
    /**
     * Explicación de cómo se calculó la estimación.
     * Útil para debugging y logs.
     */
    private String explicacionEstimacion;
    
    // ========== FACTORY METHODS ==========
    
    /**
     * Crea un análisis vacío (query no consolidable).
     */
    public static AnalisisConsolidacion crearAnalisisVacio() {
        return new AnalisisConsolidacion(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            false,
            null,
            null,
            0.0,
            "Query no es consolidable"
        );
    }
    
    /**
     * Verifica si la query es segura para cargar en memoria.
     * Una query es segura si:
     * - Es tipo AGREGADA
     * - Tiene estimación de registros < 10,000
     * - Tiene confianza de estimación >= 0.7
     */
    public boolean esSeguraParaMemoria() {
        if (tipoConsolidacion != TipoConsolidacion.AGREGACION) {
            return false;
        }
        
        if (registrosEstimados == null) {
            return false;
        }
        
        return registrosEstimados < 10000 && confianzaEstimacion >= 0.7;
    }
    
    /**
     * Verifica si la query necesita streaming obligatorio.
     */
    public boolean necesitaStreaming() {
        return tipoConsolidacion == TipoConsolidacion.CRUDO ||
               (registrosEstimados != null && registrosEstimados >= 10000);
    }
}