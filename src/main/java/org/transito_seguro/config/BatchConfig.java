package org.transito_seguro.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.batch")
public class BatchConfig {

    private int size = 1000;
    private int maxRetries = 3;
    private int timeoutSeconds = 30;
    private int maxMemoryPerBatchMB = 50;
    private boolean enableParallelProcessing = true;
    private int maxConcurrentProvinces = 3;

    // Configuraciones por tipo de consulta
    private ConsultaConfig personasJuridicas = new ConsultaConfig(2000, 5);
    private ConsultaConfig reporteGeneral = new ConsultaConfig(1000, 3);
    private ConsultaConfig infracciones = new ConsultaConfig(1500, 4);

    @Data
    public static class ConsultaConfig {
        private int batchSize;
        private int maxRetries;

        public ConsultaConfig() {}

        public ConsultaConfig(int batchSize, int maxRetries) {
            this.batchSize = batchSize;
            this.maxRetries = maxRetries;
        }
    }

    /**
     * Obtiene configuración específica para un tipo de consulta
     */
    public ConsultaConfig getConfigForTipo(String tipoConsulta) {
        switch (tipoConsulta.toLowerCase()) {
            case "personas-juridicas":
                return personasJuridicas;
            case "reporte-general":
                return reporteGeneral;
            case "infracciones":
            case "infracciones-por-equipos":
                return infracciones;
            default:
                return new ConsultaConfig(size, maxRetries);
        }
    }
}
