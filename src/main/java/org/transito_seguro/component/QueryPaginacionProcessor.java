package org.transito_seguro.component;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

/**
 * Este componente cumple la funcion de agregar una paginacion ( LIMIT & OFFSET ) a las querys
 * */
@Component
@Slf4j
public class QueryPaginacionProcessor {

    // Son patrones para detectar el LIMIT/OFF
    private static final Pattern LIMIT_PATTERN = Pattern.compile("\\bLIMIT\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern OFFSET_PATTERN = Pattern.compile("\\bOFFSET\\s+", Pattern.CASE_INSENSITIVE);

    // Sirve para detectar si al final de la query hay un ORDER
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile("\\bORDER\\s+BY\\s+[^;]+", Pattern.CASE_INSENSITIVE);


    /** Metodo principal que agrega paginacion a una query  SI ES NECESARIO
     * @param sqlOriginal original
     * @param esConsolidado = true  sin limite porque necesito todos los datos
     * @param esConsolidado = false
     * @return devuelve la query con paginacion agregada
     */
    public String agregarPaginacion(String sqlOriginal, boolean esConsolidado) {
        // Validación básica
        if (sqlOriginal == null || sqlOriginal.trim().isEmpty()) {
            return sqlOriginal;
        }

        // Si es consolidado NO agregamos limite
        if (esConsolidado) {
            return sqlOriginal;
        }

        String sql = sqlOriginal.trim();
        String sqlUpper = sql.toUpperCase();

        // Agregar LIMIT si no existe
        if (!sqlUpper.contains("LIMIT")) {
            sql += "\nLIMIT CASE WHEN :aplicarPaginacion = true THEN :limite ELSE NULL END";
        }

        // Agregar OFFSET si no existe (independiente del LIMIT)
        if (!sqlUpper.contains("OFFSET")) {
            sql += "\nOFFSET CASE WHEN :aplicarPaginacion = false THEN :offset ELSE NULL END)";
        }

        return sql;
    }

    public boolean validarPaginacion(String sql) {
        if (sql == null) return false;
        String sqlUpper = sql.toUpperCase();
        return sqlUpper.contains("LIMIT") && sqlUpper.contains("OFFSET");
    }




}
