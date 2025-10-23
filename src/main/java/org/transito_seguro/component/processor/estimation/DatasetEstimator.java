package org.transito_seguro.component.processor.estimation;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.service.QueryRegistryService;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Componente especializado en estimar el volumen de datos.
 *
 * Responsabilidades:
 * - Ejecutar COUNT(*) por provincia
 * - Calcular estadísticas (total, promedio, máximo)
 * - Construir queries de conteo
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DatasetEstimator {

    private final QueryRegistryService queryRegistryService;

    /**
     * Estima el tamaño del dataset usando COUNT(*).
     *
     * @param repositories Lista de repositorios (uno por provincia)
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @return Estimación con total, promedio y máximo
     */
    public EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        log.info("📊 Estimando dataset para query: {}", nombreQuery);

        // Ejecutar COUNT(*) en paralelo para todas las provincias
        List<Integer> conteos = repositories.parallelStream()
                .map(repo -> obtenerConteoReal(repo, nombreQuery, filtros))
                .collect(Collectors.toList());

        // Calcular estadísticas
        int totalEstimado = conteos.stream().mapToInt(Integer::intValue).sum();
        double promedio = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximo = conteos.stream().mapToInt(Integer::intValue).max().orElse(0);

        log.info("✅ Estimación completada: Total={}, Promedio={:.0f}, Máximo={}",
                totalEstimado, promedio, maximo);

        return new EstimacionDataset(totalEstimado, promedio, maximo);
    }

    /**
     * Obtiene el conteo real de registros usando COUNT(*).
     *
     * @param repo Repositorio de la provincia
     * @param nombreQuery Código de la query
     * @param filtros Filtros aplicados
     * @return Número exacto de registros
     */
    private int obtenerConteoReal(
            InfraccionesRepositoryImpl repo,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {
        try {
            Optional<QueryStorage> queryOpt = queryRegistryService.buscarQuery(nombreQuery);
            if (!queryOpt.isPresent()) {
                log.warn("⚠️ Query no encontrada: {} para {}", nombreQuery, repo.getProvincia());
                return 0;
            }

            QueryStorage queryStorage = queryOpt.get();
            String queryOriginal = queryStorage.getSqlQuery();
            String queryConteo = construirQueryConteo(queryOriginal);

            Integer conteoReal = repo.ejecutarQueryConteo(queryConteo, filtros);

            log.debug("🔍 Conteo para {}: {} registros", repo.getProvincia(), conteoReal);

            return conteoReal != null ? conteoReal : 0;

        } catch (Exception e) {
            log.error("❌ Error obteniendo conteo para {}: {}",
                    repo.getProvincia(), e.getMessage());
            return 0;
        }
    }

    /**
     * Construye query de conteo removiendo ORDER BY y LIMIT.
     *
     * @param queryOriginal Query SQL original
     * @return Query envuelta en SELECT COUNT(*)
     */
    public String construirQueryConteo(String queryOriginal) {
        if (queryOriginal == null || queryOriginal.trim().isEmpty()) {
            throw new IllegalArgumentException("Query original no puede estar vacía");
        }

        log.debug("🔢 Construyendo query de conteo");

        String queryLimpia = queryOriginal.trim();

        // Remover punto y coma final
        if (queryLimpia.endsWith(";")) {
            queryLimpia = queryLimpia.substring(0, queryLimpia.length() - 1).trim();
        }

        // Remover cláusulas externas
        queryLimpia = removerClausulaExterna(queryLimpia, "ORDER BY");
        queryLimpia = removerClausulaExterna(queryLimpia, "LIMIT");
        queryLimpia = removerClausulaExterna(queryLimpia, "OFFSET");

        // Envolver en COUNT
        String queryConteo = String.format(
                "SELECT COUNT(*) as total FROM (%s) AS conteo_wrapper",
                queryLimpia.trim()
        );

        log.debug("✅ Query de conteo construida: {} caracteres", queryConteo.length());

        return queryConteo;
    }

    /**
     * Remueve una cláusula del nivel externo (no dentro de paréntesis).
     */
    private String removerClausulaExterna(String query, String clausula) {
        int nivelParentesis = 0;
        int posClausula = -1;
        String upper = query.toUpperCase();
        String clausulaUpper = clausula.toUpperCase();

        // Recorrer desde el final hacia el inicio
        for (int i = query.length() - 1; i >= 0; i--) {
            char c = query.charAt(i);

            if (c == ')') {
                nivelParentesis++;
            } else if (c == '(') {
                nivelParentesis--;
            }

            // Buscar la cláusula solo en nivel 0
            if (nivelParentesis == 0 && i >= clausulaUpper.length()) {
                boolean coincide = true;
                for (int j = 0; j < clausulaUpper.length(); j++) {
                    if (upper.charAt(i - clausulaUpper.length() + j) != clausulaUpper.charAt(j)) {
                        coincide = false;
                        break;
                    }
                }

                if (coincide) {
                    int posInicio = i - clausulaUpper.length();
                    boolean esLimitePalabra = true;

                    if (posInicio > 0 && Character.isLetterOrDigit(query.charAt(posInicio - 1))) {
                        esLimitePalabra = false;
                    }

                    if (i < query.length() && Character.isLetterOrDigit(query.charAt(i))) {
                        esLimitePalabra = false;
                    }

                    if (esLimitePalabra) {
                        posClausula = posInicio;
                        break;
                    }
                }
            }
        }

        if (posClausula >= 0) {
            log.debug("🔧 Removiendo {} externo en posición {}", clausula, posClausula);
            return query.substring(0, posClausula).trim();
        }

        return query;
    }
}