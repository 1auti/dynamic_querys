package org.transito_seguro.component;

import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.HashMap;
import java.util.Map;

@Component
public class ParametrosProcessor {

    /**
     * Procesa los filtros y genera condiciones SQL dinámicas
     */
    public Map<String, Object> procesarFiltros(ParametrosFiltrosDTO filtros) {
        Map<String, Object> resultado = new HashMap<>();

        StringBuilder whereClause = new StringBuilder(" WHERE 1=1 ");
        Map<String, Object> parametros = new HashMap<>();

        // Procesamiento de fechas
        if (filtros.getUsarFechaEspecifica() != null && filtros.getUsarFechaEspecifica()) {
            whereClause.append(" AND DATE(fecha_infraccion) = DATE(:fechaEspecifica) ");
            parametros.put("fechaEspecifica", filtros.getFechaEspecifica());
        } else {
            if (filtros.getFechaInicio() != null) {
                whereClause.append(" AND fecha_infraccion >= :fechaInicio ");
                parametros.put("fechaInicio", filtros.getFechaInicio());
            }
            if (filtros.getFechaFin() != null) {
                whereClause.append(" AND fecha_infraccion <= :fechaFin ");
                parametros.put("fechaFin", filtros.getFechaFin());
            }
        }

        // Procesamiento de bases de datos específicas
        if (filtros.getUsarTodasLasBDS() == null || !filtros.getUsarTodasLasBDS()) {
            if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
                whereClause.append(" AND provincia IN (:provincias) ");
                parametros.put("provincias", filtros.getBaseDatos());
            }
        }

        resultado.put("whereClause", whereClause.toString());
        resultado.put("parametros", parametros);
        resultado.put("tieneCondicionesDinamicas", parametros.size() > 0);

        return resultado;
    }

    /**
     * Genera fragmentos SQL reutilizables
     */
    public String generarOrderBy(String campo, String direccion) {
        return String.format(" ORDER BY %s %s ", campo, direccion != null ? direccion : "ASC");
    }

    public String generarLimit(Integer limite, Integer offset) {
        if (limite != null) {
            String limitClause = " LIMIT " + limite;
            if (offset != null) {
                limitClause += " OFFSET " + offset;
            }
            return limitClause;
        }
        return "";
    }
}
