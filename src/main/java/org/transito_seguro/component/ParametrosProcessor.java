package org.transito_seguro.component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.Map;


@Slf4j
@Component
public class ParametrosProcessor {

    @Getter
    public static class QueryResult {
        private final String queryModificada;
        private final MapSqlParameterSource parametros;
        private final Map<String, Object> metadata;

        public QueryResult(String queryModificada, MapSqlParameterSource parametros, Map<String, Object> metadata) {
            this.queryModificada = queryModificada;
            this.parametros = parametros;
            this.metadata = metadata;
        }
    }

    private void mapearParametroFechas(ParametrosFiltrosDTO filtrosDTO, MapSqlParameterSource sqlParameterSource){
        sqlParameterSource.addValue("fechaInicio",filtrosDTO.getFechaInicio());
        sqlParameterSource.addValue("fechaFin",filtrosDTO.getFechaFin());
        sqlParameterSource.addValue("fechaEspecifica",filtrosDTO.getFechaEspecifica());
    }

    private void mapearPaginacion(ParametrosFiltrosDTO filtrosDTO,MapSqlParameterSource sqlParameterSource){
        sqlParameterSource.addValue("limite",filtrosDTO.getLimiteMaximo());
        sqlParameterSource.addValue("offset",filtrosDTO.calcularOffset());
    }




}