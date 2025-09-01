package org.transito_seguro.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StreamUtils;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SqlUtils {

    public static String cargarQuery(String nombreArchivoSql){
        try {
            String rutaQuery = String.format("querys/%s", nombreArchivoSql);
            ClassPathResource resource = new ClassPathResource(rutaQuery);
            return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error cargando query " + nombreArchivoSql, e);
        }
    }

    public static MapSqlParameterSource construirParametros(ParametrosFiltrosDTO filtros){
        MapSqlParameterSource params = new MapSqlParameterSource();

        if(filtros.getFechaEspecifica() != null){
            params.addValue("fechaEspecifica",filtros.getFechaEspecifica());
            params.addValue("usarFechaEspecifica",true);
        }else if (filtros.getFechaInicio() != null || filtros.getFechaFin() != null){
            params.addValue("FechaInicio",filtros.getFechaInicio());
            params.addValue("FechaFin",filtros.getFechaFin());
            params.addValue("userFechaEspecifica",false);
        }

        if(filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()){
            params.addValue("usarTodasLasBDS",true);
        }else if (filtros.getUsarTodasLasBDS() != null){
            params.addValue("baseDatos",filtros.getBaseDatos());
        }

        return params;
    }
}
