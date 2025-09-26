package org.transito_seguro.domain;

import org.transito_seguro.enums.TipoCampo;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CamposPorCategoria {

    private CamposPorCategoria(){};

    // Metodo auxillar para crear el mapa con categorias
    private static Map<String, TipoCampo> createCategorizedMap(TipoCampo categoria, String... keys) {
        // 1. Recolecta las claves en un mapa mutable estándar.
        Map<String, TipoCampo> mutableMap = Stream.of(keys)
                .collect(Collectors.toMap(
                        key -> key,      // La clave es la propia cadena
                        key -> categoria // El valor es siempre la categoría pasada como argumento
                ));

        // 2. Devuelve una vista inmutable de ese mapa.
        return Collections.unmodifiableMap(mutableMap);
    }


    public static final Map<String, TipoCampo> CAMPOS_UBICACION = createCategorizedMap(
            TipoCampo.UBICACION,
            "provincia", "municipio", "contexto", "lugar", "partido", "localidad"
    );

    public static final Map<String, TipoCampo> CAMPOS_CATEGORIZACION = createCategorizedMap(
            TipoCampo.CATEGORIZACION,
            "tipo_infraccion", "serie_equipo", "descripcion", "estado", "tipo_documento", "vehiculo"
    );

    public static final Map<String, TipoCampo> CAMPOS_TIEMPO = createCategorizedMap(
            TipoCampo.TIEMPO,
            "fecha", "fecha_reporte", "fecha_emision", "fecha_alta", "fecha_titularidad",
            "ultima_modificacion", "fecha_constatacion", "mes_exportacion"
    );

    public static final Map<String, TipoCampo> CAMPOS_NUMERICOS_CONOCIDOS = createCategorizedMap(
            TipoCampo.NUMERICO_SUMA,
            "total", "cantidad", "vehiculos", "motos", "formato_no_valido", "counter",
            "luz_roja", "senda", "total_sin_email", "velocidad_radar_fijo",
            "pre_aprobada", "filtrada_municipio", "sin_datos", "prescrita", "filtrada",
            "aprobada", "pdf_generado", "total_eventos"
    );


}
