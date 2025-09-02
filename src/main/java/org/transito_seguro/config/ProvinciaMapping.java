package org.transito_seguro.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "provincia")
public class ProvinciaMapping {

    private Map<String, String> mapping;

    /**
     * Obtener el mapeo de provincias a datasources
     */
    public Map<String, String> getMapping() {
        return mapping;
    }

    /**
     * Verificar si una provincia está soportada
     */
    public boolean isProvinciaSupported(String provincia) {
        return mapping != null && mapping.containsKey(provincia);
    }

    /**
     * Obtener datasource para una provincia
     */
    public String getDatasourceForProvincia(String provincia) {
        return mapping != null ? mapping.get(provincia) : null;
    }
}