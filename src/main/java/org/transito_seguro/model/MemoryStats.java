package org.transito_seguro.model;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.transito_seguro.enums.MemoryLevel;

/**
 * Clase interna para estadísticas de memoria.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public  class MemoryStats {
    private  long memoriaMaxima;
    private  long memoriaTotal;
    private  long memoriaUsada;
    private  long memoriaLibre;
    private  double porcentajeUsado;
    private MemoryLevel nivel;

    @Override
    public String toString() {
        return String.format("%s Memoria: %.1f%% usado (%dMB/%dMB)",
                nivel.getEmoji(),
                porcentajeUsado,
                memoriaUsada / 1024 / 1024,
                memoriaMaxima / 1024 / 1024);
    }

    /**
     * Retorna información detallada con recomendaciones.
     */
    public String toStringDetallado() {
        return String.format(
                "%s Memoria: %.1f%% usado%n" +
                        "  Usada: %dMB / Total: %dMB / Máxima: %dMB%n" +
                        "  Libre: %dMB%n" +
                        "  %s",
                nivel.getEmoji(),
                porcentajeUsado,
                memoriaUsada / 1024 / 1024,
                memoriaTotal / 1024 / 1024,
                memoriaMaxima / 1024 / 1024,
                memoriaLibre / 1024 / 1024,
                nivel.getDescripcion()
        );
    }
}