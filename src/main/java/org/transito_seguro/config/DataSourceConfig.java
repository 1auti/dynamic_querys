package org.transito_seguro.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import javax.sql.DataSource;

@Configuration
@Slf4j
public class DataSourceConfig {

    @Primary
    @Bean(name = "primaryDataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource primaryDataSource() {
        log.info("Configurando H2 DataSource primario para QueryMetadata");
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(name = "pbaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.pba")
    public DataSource pbaDataSource() {
        log.info("Configurando PBA Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "mdaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.mda")
    public DataSource mdaDataSource() {
        log.info("Configurando MDA Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "santa-rosaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.santa-rosa")
    public DataSource santaRosaDataSource() {
        log.info("Configurando SANTA_ROSA Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "chacoDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.chaco")
    public DataSource chacoDataSource() {
        log.info("Configurando CHACO Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "entre-riosDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.entre-rios")
    public DataSource entreRiosDataSource() {
        log.info("Configurando ENTRE_RIOS Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "formosaDataSource")
    @ConfigurationProperties(prefix = "postgresql.datasource.formosa")
    public DataSource formosaDataSource() {
        log.info("Configurando FORMOSA Database");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    // DataSource por defecto (opcional, puedes usar cualquiera)
    @Primary
    @Bean(name = "defaultDataSource")
    public DataSource defaultDataSource() {
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }
}