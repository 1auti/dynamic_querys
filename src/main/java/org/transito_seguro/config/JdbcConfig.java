package org.transito_seguro.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class JdbcConfig {

    private static final int FETCH_SIZE = 1000; // Para streaming eficiente

    @Bean(name = "pbaJdbcTemplate")
    public NamedParameterJdbcTemplate pbaJdbcTemplate(@Qualifier("pbaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Buenos Aires");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }

    @Bean(name = "mdaJdbcTemplate")
    public NamedParameterJdbcTemplate mdaJdbcTemplate(@Qualifier("mdaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Avellaneda");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }

    @Bean(name = "santa-rosaJdbcTemplate")
    public NamedParameterJdbcTemplate santaRosaJdbcTemplate(@Qualifier("santa-rosaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: La Pampa");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }

    @Bean(name = "chacoJdbcTemplate")
    public NamedParameterJdbcTemplate chacoJdbcTemplate(@Qualifier("chacoDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Chaco");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }

    @Bean(name = "entre-riosJdbcTemplate")
    public NamedParameterJdbcTemplate entreRiosJdbcTemplate(@Qualifier("entre-riosDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Entre Ríos");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }

    @Bean(name = "formosaJdbcTemplate")
    public NamedParameterJdbcTemplate formosaJdbcTemplate(@Qualifier("formosaDataSource") DataSource dataSource) {
        log.debug("Configurando JdbcTemplate: Formosa");
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        template.getJdbcTemplate().setFetchSize(FETCH_SIZE);
        return template;
    }
}