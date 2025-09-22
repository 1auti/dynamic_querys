package org.transito_seguro.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

/**
 * Ejecutador de querys para las BDS
 */
@Configuration
@Slf4j
public class JdbcConfig {

    @Bean(name = "pbaJdbcTemplate")
    public NamedParameterJdbcTemplate pbaJdbcTemplate(@Qualifier("pbaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Buenos Aires");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "mdaJdbcTemplate")
    public NamedParameterJdbcTemplate mdaJdbcTemplate(@Qualifier("mdaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Avellaneda");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "santa-rosaJdbcTemplate")
    public NamedParameterJdbcTemplate santaRosaJdbcTemplate(@Qualifier("santa-rosaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: La Pampa");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "chacoJdbcTemplate")
    public NamedParameterJdbcTemplate chacoJdbcTemplate(@Qualifier("chacoDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Chaco");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "entre-riosJdbcTemplate")
    public NamedParameterJdbcTemplate entreRiosJdbcTemplate(@Qualifier("entre-riosDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Entre Ríos");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "formosaJdbcTemplate")
    public NamedParameterJdbcTemplate formosaJdbcTemplate(@Qualifier("formosaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Formosa");
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "defaultJdbcTemplate")
    public NamedParameterJdbcTemplate defaultJdbcTemplate(@Qualifier("defaultDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate por defecto (H2)");
        return new NamedParameterJdbcTemplate(dataSource);
    }


}