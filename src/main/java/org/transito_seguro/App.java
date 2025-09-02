package org.transito_seguro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Clase principal de la aplicación Spring Boot
 * Sistema de Consulta Dinámica de Infracciones de Tránsito
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableTransactionManagement
@EnableAsync
@EnableAspectJAutoProxy
public class App {

    public static void main(String[] args) {
        System.out.println("🚀 Iniciando Sistema de Tránsito Seguro...");
        SpringApplication.run(App.class, args);
        System.out.println("✅ Sistema iniciado correctamente!");
    }
}