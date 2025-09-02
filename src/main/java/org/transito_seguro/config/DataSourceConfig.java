package org.transito_seguro.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean(name = "pbaDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.pba")
    public DataSource pbaDataSource() {
        return new HikariDataSource();
    }

    @Bean(name = "mdaDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.mda")
    public DataSource mdaDataSource() {
        return new HikariDataSource();
    }

    @Bean(name = "santa-rosaDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.santa-rosa")
    public DataSource santaRosaDataSource() {
        return new HikariDataSource();
    }

    @Bean(name = "chacoDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.chaco")
    public DataSource chacoDataSource() {
        return new HikariDataSource();
    }

    @Bean(name = "entre-riosDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.entre-rios")
    public DataSource entreRiosDataSource() {
        return new HikariDataSource();
    }

    @Bean(name = "formosaDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.formosa")
    public DataSource formosaDataSource() {
        return new HikariDataSource();
    }

    // DataSource por defecto (opcional, puedes usar cualquiera)
    @Primary
    @Bean(name = "defaultDataSource")
    public DataSource defaultDataSource() {
        return pbaDataSource();
    }
}