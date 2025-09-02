package org.transito_seguro.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StreamUtils;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlUtils {

    // Cache para queries cargadas
    private static final Map<String, String> queryCache = new ConcurrentHashMap<>();

    /**
     * Carga una query desde el sistema de archivos con cache
     */
    public static String cargarQuery(String nombreArchivoSql) {
        return queryCache.computeIfAbsent(nombreArchivoSql, SqlUtils::cargarQueryDesdeArchivo);
    }

    /**
     * Carga la query desde el archivo físicamente
     */
    private static String cargarQueryDesdeArchivo(String nombreArchivoSql) {
        try {
            String rutaQuery = String.format("querys/%s", nombreArchivoSql);
            ClassPathResource resource = new ClassPathResource(rutaQuery);
            return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error cargando query " + nombreArchivoSql, e);
        }
    }

    /**
     * Construye parámetros básicos (DEPRECADO - usar ParametrosProcessor)
     * @deprecated Use ParametrosProcessor.procesarQuery() instead
     */
    @Deprecated
    public static MapSqlParameterSource construirParametros(ParametrosFiltrosDTO filtros) {
        MapSqlParameterSource params = new MapSqlParameterSource();

        if (filtros.getFechaEspecifica() != null) {
            params.addValue("fechaEspecifica", filtros.getFechaEspecifica());
            params.addValue("usarFechaEspecifica", true);
        } else if (filtros.getFechaInicio() != null || filtros.getFechaFin() != null) {
            params.addValue("FechaInicio", filtros.getFechaInicio());
            params.addValue("FechaFin", filtros.getFechaFin());
            params.addValue("userFechaEspecifica", false);
        }

        if (filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()) {
            params.addValue("usarTodasLasBDS", true);
        } else if (filtros.getUsarTodasLasBDS() != null) {
            params.addValue("baseDatos", filtros.getBaseDatos());
        }

        return params;
    }

    /**
     * Limpia el cache de queries (útil para testing o hot reload)
     */
    public static void limpiarCache() {
        queryCache.clear();
    }

    /**
     * Verifica si una query existe en el sistema de archivos
     */
    public static boolean existeQuery(String nombreArchivoSql) {
        try {
            String rutaQuery = String.format("querys/%s", nombreArchivoSql);
            ClassPathResource resource = new ClassPathResource(rutaQuery);
            return resource.exists();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Carga múltiples queries de una vez
     */
    public static Map<String, String> cargarQueries(String... nombresArchivos) {
        Map<String, String> queries = new ConcurrentHashMap<>();
        for (String nombre : nombresArchivos) {
            queries.put(nombre, cargarQuery(nombre));
        }
        return queries;
    }
}