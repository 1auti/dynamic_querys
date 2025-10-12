package org.transito_seguro.model.query;

import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import lombok.Getter;
import lombok.Setter;
import lombok.AllArgsConstructor;

@Getter
@Setter
@AllArgsConstructor
public class QueryResult{

        private final String queryModificada;
        private final MapSqlParameterSource parametros;
        private final Map<String, Object> metadata;

    
}