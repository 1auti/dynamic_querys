package org.transito_seguro.config;

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
public class JdbcConfig {

    @Bean(name = "pbaJdbcTemplate")
    public NamedParameterJdbcTemplate pbaJdbcTemplate(@Qualifier("pbaDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "mdaJdbcTemplate")
    public NamedParameterJdbcTemplate mdaJdbcTemplate(@Qualifier("mdaDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "santaRosaJdbcTemplate")
    public NamedParameterJdbcTemplate santaRosaJdbcTemplate(@Qualifier("santaRosaDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "chacoJdbcTemplate")
    public NamedParameterJdbcTemplate chacoJdbcTemplate(@Qualifier("chacoDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "entreRiosJdbcTemplate")
    public NamedParameterJdbcTemplate entreRiosJdbcTemplate(@Qualifier("entreRiosDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "formosaJdbcTemplate")
    public NamedParameterJdbcTemplate formosaJdbcTemplate(@Qualifier("formosaDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    // JdbcTemplate por defecto (opcional)
    @Primary
    @Bean(name = "defaultJdbcTemplate")
    public NamedParameterJdbcTemplate defaultJdbcTemplate(@Qualifier("defaultDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }
}
