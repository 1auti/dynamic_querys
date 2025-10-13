package org.transito_seguro.service;



import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.query.QueryResult;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Servicio para ejecutar queries SQL en repositorios de provincias.
 *
 * Responsabilidades:
 * - Ejecutar COUNT(*) para estimación de dataset
 * - Ejecutar queries SQL con parámetros
 * - Recopilar datos de múltiples provincias
 *
 * Este servicio NO tiene lógica de análisis ni consolidación,
 * solo ejecución pura de SQL.
 *
 * @author Sistema Tránsito Seguro
 * @version 1.0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryExecutionService {

    private final ParametrosProcessor parametrosProcessor;
    private final QueryRegistryService queryRegistryService;

    /**
     * Estima el tamaño total del dataset ejecutando COUNT(*) en todas las provincias.
     *
     * @param repositories Lista de repositorios de provincias
     * @param nombreQuery Código de la query
     * @return Estimación con total, promedio y máximo
     */
    public EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery) {

        log.debug("Estimando dataset para query: {}", nombreQuery);

        // Ejecutar COUNT(*) en paralelo para todas las provincias
        List<Integer> conteos = repositories.parallelStream()
                .map(repo -> obtenerConteoReal(repo, nombreQuery))
                .collect(Collectors.toList());

        // Calcular estadísticas
        int totalEstimado = conteos.stream().mapToInt(Integer::intValue).sum();
        double promedio = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximo = conteos.stream().mapToInt(Integer::intValue).max().orElse(0);

        log.info("Estimación completada - Total: {}, Promedio: {:.0f}, Máximo: {}",
                totalEstimado, promedio, maximo);

        return new EstimacionDataset(totalEstimado, promedio, maximo);
    }

    /**
     * Obtiene el conteo real de registros para una provincia específica.
     *
     * @param repo Repositorio de la provincia
     * @param nombreQuery Código de la query
     * @return Cantidad de registros (0 si hay error)
     */
    private int obtenerConteoReal(
            InfraccionesRepositoryImpl repo,
            String nombreQuery) {
        try {


            // Construir query COUNT(*) envolviendo en subquery
            String queryConteo = construirQueryConteo(nombreQuery);

            // Ejecutar COUNT(*) directamente
            Integer conteoReal = repo.ejecutarQueryConteoDesdeSQL(queryConteo);

            log.debug("Conteo REAL para {}: {} registros (estimación previa: {})",
                    repo.getProvincia(),
                    conteoReal);

            return conteoReal != null ? conteoReal : 0;

        } catch (Exception e) {
            log.error("Error obteniendo conteo para {} - {}: {}",
                    repo.getProvincia(), nombreQuery, e.getMessage());
            return 0;
        }
    }

    /**
     * Construye una query COUNT(*) envolviendo la query original en un subquery.
     *
     * Antes: SELECT campo1, campo2 FROM tabla WHERE condicion ORDER BY campo
     * Después: SELECT COUNT(*) as total FROM (SELECT campo1, campo2 FROM tabla WHERE condicion) AS subquery
     *
     * @param queryOriginal Query SQL original
     * @return Query COUNT(*) válida
     */
    private String construirQueryConteo(String queryOriginal) {
        // Remover ORDER BY si existe (no necesario para contar)
        String querySinOrder = queryOriginal.replaceAll("(?i)ORDER BY[^;]*", "").trim();

        // Envolver en subquery para COUNT(*)
        return String.format("SELECT COUNT(*) as total FROM (%s) AS subquery", querySinOrder);
    }

    /**
     * Ejecuta una query SQL en un repositorio específico con filtros.
     *
     * @param repo Repositorio de la provincia
     * @param sqlQuery Query SQL a ejecutar
     * @param filtros Filtros aplicados
     * @return Lista de registros resultantes
     */
    public List<java.util.Map<String, Object>> ejecutarQueryEnRepositorio(
            InfraccionesRepositoryImpl repo,
            String sqlQuery,
            ParametrosFiltrosDTO filtros) {

        try {
            // Procesar query con parámetros
            QueryResult resultado = parametrosProcessor.procesarQuery(sqlQuery, filtros);

            // Ejecutar en el JdbcTemplate del repositorio
            return repo.getNamedParameterJdbcTemplate().queryForList(
                    resultado.getQueryModificada(),
                    resultado.getParametros()
            );

        } catch (Exception e) {
            log.error("Error ejecutando query en provincia {}: {}",
                    repo.getProvincia(), e.getMessage());
            throw new RuntimeException("Error ejecutando query en " + repo.getProvincia(), e);
        }
    }

    /**
     * Valida la sintaxis SQL de una query removiendo condiciones WHERE incompletas.
     *
     * @param query Query original
     * @return Query limpia y válida
     */
    public String limpiarQueryParaCount(String query) {
        // Remover líneas WHERE sin condición (ej: "WHERE campo_solo\n AND (...)")
        String limpia = query.replaceAll(
                "WHERE\\s+[\\w.]+\\s+(?=AND|OR|GROUP|ORDER|LIMIT|$)",
                "WHERE 1=1 "
        );

        // Remover LIMITs para count completo
        limpia = limpia.replaceAll("LIMIT\\s+[^;]+", "");

        return limpia;
    }
}
