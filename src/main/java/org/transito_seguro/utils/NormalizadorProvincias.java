package org.transito_seguro.utils;

import java.util.HashMap;
import java.util.Map;

public final class NormalizadorProvincias {
    public static final Map<String, String> MAPEO_PROVINCIAS = new HashMap<>();

    static {
        MAPEO_PROVINCIAS.put("Buenos Aires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("BuenosAires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("LaPampa", "La Pampa");
        MAPEO_PROVINCIAS.put("Chaco", "Chaco");
        MAPEO_PROVINCIAS.put("EntreRos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Entre Ríos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Formosa", "Formosa");
    }

    private NormalizadorProvincias() {}

    public static String normalizar(String valor) {
        return MAPEO_PROVINCIAS.getOrDefault(valor, valor);
    }
}

