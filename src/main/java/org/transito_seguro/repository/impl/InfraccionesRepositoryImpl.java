package org.transito_seguro.repository.impl;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.utils.SqlUtils;
import java.util.List;
import java.util.Map;


public class InfraccionesRepositoryImpl implements InfraccionesRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String provincia;

    public InfraccionesRepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate, String provincia) {
        this.jdbcTemplate = jdbcTemplate;
        this.provincia = provincia;
    }

    @Override
    public List<Map<String, Object>> consultarPersonasJuridicas(ParametrosFiltrosDTO filtro) {
        String query = SqlUtils.cargarQuery("consultar_personas_juridicas.sql");
        MapSqlParameterSource params = SqlUtils.construirParametros(filtro);

        List<Map<String,Object>> resultados =jdbcTemplate.queryForList(query,params);

        return resultados;
    }



}
