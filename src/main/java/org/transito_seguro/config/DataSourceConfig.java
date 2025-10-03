package org.transito_seguro.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class DataSourceConfig {

    // =============== H2 DATASOURCE PRIMARIO ===============
    @Primary
    @Bean(name = "primaryDataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource primaryDataSource() {
        log.info("Configurando H2 DataSource primario para QueryMetadata");
        return new HikariDataSource();
    }

    // =============== POSTGRESQL DATASOURCES ===============

    @Bean(name = "pbaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.pba")
    public DataSource pbaDataSource() {
        log.info("Configurando PBA PostgreSQL DataSource");
        return new HikariDataSource();
    }

    @Bean(name = "mdaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.mda")
    public DataSource mdaDataSource() {
        log.info("Configurando MDA PostgreSQL DataSource");
        return new HikariDataSource();
    }

    @Bean(name = "santa-rosaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.santa-rosa")
    public DataSource santaRosaDataSource() {
        log.info("Configurando Santa Rosa PostgreSQL DataSource");
        return new HikariDataSource();
    }

    @Bean(name = "chacoDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.chaco")
    public DataSource chacoDataSource() {
        log.info("Configurando Chaco PostgreSQL DataSource");
        return new HikariDataSource();
    }

    @Bean(name = "entre-riosDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.entre-rios")
    public DataSource entreRiosDataSource() {
        log.info("Configurando Entre Ríos PostgreSQL DataSource");
        return new HikariDataSource();
    }

    @Bean(name = "formosaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasources.formosa")
    public DataSource formosaDataSource() {
        log.info("Configurando Formosa PostgreSQL DataSource");
        return new HikariDataSource();
    }
}