package org.transito_seguro.component.processor.execution;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

/**
 * Ejecutor principal de queries por provincia.
 *
 * Responsabilidades:
 * - Determinar tipo de ejecución (consolidada vs estándar)
 * - Delegar a ejecutor especializado
 * - Manejar excepciones y logging
 *
 * Delega a:
 * - {@link ConsolidatedQueryExecutor}: Queries consolidadas (GROUP BY)
 * - {@link StandardQueryExecutor}: Queries estándar (con paginación)
 *
 * @author Transito Seguro Team
 * @version 3.0 - Extraído de BatchProcessor
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QueryExecutor {

    private final ConsolidatedQueryExecutor consolidatedExecutor;
    private final StandardQueryExecutor standardExecutor;

    /**
     * Ejecuta una provincia decidiendo la estrategia según el tipo de query.
     *
     * @param repo Repositorio de la provincia
     * @param filtros Filtros aplicados
     * @param nombreQuery Código de la query
     * @param provincia Nombre de la provincia
     * @param contexto Contexto de procesamiento
     * @param queryStorage Metadata de la query
     */
    public void ejecutarProvincia(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        try {
            // Verificar si la estrategia permite ejecución sin paginación
            boolean estrategiaSinPaginacion =
                    queryStorage.getEstrategiaPaginacion() == EstrategiaPaginacion.SIN_PAGINACION;

            // Si es consolidable Y la estrategia lo permite, usar ejecutor consolidado
            if (estrategiaSinPaginacion && Boolean.TRUE.equals(queryStorage.getEsConsolidable())) {
                log.debug("📊 Query consolidable detectada para {}: tipo={}",
                        provincia, queryStorage.getTipoConsolidacion());

                // Delegar a ejecutor consolidado
                consolidatedExecutor.ejecutar(
                        repo,
                        filtros,
                        nombreQuery,
                        provincia,
                        contexto,
                        queryStorage
                );

            } else {
                // Query estándar: usar paginación
                log.debug("📄 Query estándar para {}: usando paginación", provincia);

                // Delegar a ejecutor estándar
                standardExecutor.ejecutar(
                        repo,
                        filtros,
                        nombreQuery,
                        provincia,
                        contexto,
                        queryStorage
                );
            }

        } catch (OutOfMemoryError oom) {
            log.error("💥 OOM procesando {}: {}", provincia, oom.getMessage());
            throw new RuntimeException("OutOfMemoryError en provincia: " + provincia, oom);

        } catch (Exception e) {
            log.error("❌ Error ejecutando query en {}: {}", provincia, e.getMessage(), e);
            throw new RuntimeException("Error en provincia: " + provincia, e);
        }
    }
}