package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class BathResultDTO {

    private String provincia;
    private Integer numeroBatch;
    private Integer registrosProcesados;
    private Boolean exitoso;
    private String mensajeError;
    private List<Map<String, Object>> datos;
    private Long tiempoProcesamientoMs;

    /**
     * Crea un resultado exitoso
     */
    public static BathResultDTO exito(String provincia, Integer batch,
                                    List<Map<String, Object>> datos) {
        return BathResultDTO.builder()
                .provincia(provincia)
                .numeroBatch(batch)
                .registrosProcesados(datos.size())
                .exitoso(true)
                .datos(datos)
                .build();
    }

    /**
     * Crea un resultado con error
     */
    public static BathResultDTO error(String provincia, Integer batch,
                                    String mensajeError) {
        return BathResultDTO.builder()
                .provincia(provincia)
                .numeroBatch(batch)
                .registrosProcesados(0)
                .exitoso(false)
                .mensajeError(mensajeError)
                .build();
    }
}
