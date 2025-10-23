package org.transito_seguro.service;



import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.analyzer.QueryAnalyzer;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.EstimacionDataset;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Servicio para análisis dinámico de queries con verificación de volumen.
 *
 * Funcionalidades:
 * - Ejecutar COUNT(*) para queries crudas
 * - Decidir estrategia según volumen real
 * - Reescribir queries para forzar agregación cuando sea necesario
 *
 * @author Sistema Tránsito Seguro
 * @version 1.0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryAnalisisService {

    private final JdbcTemplate jdbcTemplate;
    private final QueryAnalyzer queryAnalyzer;
    private final  QueryRegistryService queryRegistryService;
    private final QueryExecutionService executionService;

    /**
     * Analiza una query y determina la estrategia óptima ejecutando
     * COUNT(*) si es necesario para queries crudas.
     *
     * Flujo de decisión:
     * 1. Análisis estático con QueryAnalyzer
     * 2. Si es CRUDO → ejecutar COUNT(*)
     * 3. Según COUNT(*) → ajustar estrategia
     * 4. Si volumen muy alto → reescribir query con GROUP BY automático
     *
     * @param query Query SQL original
     * @return Análisis completo con estrategia definitiva
     */
    public AnalisisConsolidacion analizarConVerificacionDinamica(List<InfraccionesRepositoryImpl> repositories,
                                                                 String nombreQuery) {
        log.info("=== ANÁLISIS DINÁMICO DE QUERY ===");


        // PASO 1: Análisis estático
        AnalisisConsolidacion analisisEstatico = queryAnalyzer.analizarParaConsolidacion(nombreQuery);

        // Si no es consolidable o ya es agregada, retornar análisis estático
        if (!analisisEstatico.isEsConsolidable() ||
                analisisEstatico.getTipoConsolidacion() != TipoConsolidacion.CRUDO) {

            log.info("Query no requiere verificación dinámica - Tipo: {}",
                    analisisEstatico.getTipoConsolidacion());
            return analisisEstatico;
        }

        // PASO 2: Query CRUDA detectada → Ejecutar COUNT(*)
        log.warn("Query CRUDA detectada - Ejecutando COUNT(*) para verificar volumen...");

        try {
            EstimacionDataset estimarDataset = executionService.estimarDataset(repositories,nombreQuery);
            log.info("COUNT(*) ejecutado - Resultado: {} registros", estimarDataset.getTotalEstimado());

            // PASO 3: Decidir estrategia según volumen real
            return decidirEstrategiaSegunVolumen(
                    analisisEstatico,
                    nombreQuery,
                    estimarDataset.getTotalEstimado()
            );

        } catch (Exception e) {
            log.error("Error ejecutando COUNT(*): {}", e.getMessage(), e);
            // En caso de error, usar estrategia conservadora (streaming)
            return crearAnalisisConservador(analisisEstatico);
        }
    }

    /**
     * Ejecuta COUNT(*) sobre una query para determinar cuántos registros retornará.
     *
     * Estrategia:
     * - Convierte SELECT ... en SELECT COUNT(*) ...
     * - Mantiene WHERE, JOIN y demás filtros
     * - Ignora ORDER BY, LIMIT (no afectan el conteo)
     *
     * @param query Query SQL original
     * @return Cantidad de registros que retornará la query
     */
    /**
     * Ejecuta COUNT(*) sobre una query validando sintaxis SQL
     * @param queryOriginal Query SQL a analizar
     * @return Número de registros o estimación conservadora
     */
    private EstimacionDataset estimarDataset(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery) {

        // Ejecutar COUNT(*) en paralelo para todas las provincias
        List<Integer> conteos = repositories.parallelStream()
                .map(repo -> obtenerConteoReal(repo, nombreQuery))
                .collect(Collectors.toList());

        // Calcular estadísticas
        int totalEstimado = conteos.stream().mapToInt(Integer::intValue).sum();
        double promedio = repositories.isEmpty() ? 0 : (double) totalEstimado / repositories.size();
        int maximo = conteos.stream().mapToInt(Integer::intValue).max().orElse(0);

        return new EstimacionDataset(totalEstimado, promedio, maximo);
    }

    private int obtenerConteoReal(
            InfraccionesRepositoryImpl repo,
            String nombreQuery) {
        try {
            Optional<QueryStorage> queryOpt = queryRegistryService.buscarQuery(nombreQuery);
            if (!queryOpt.isPresent()) {
                log.warn("Query no encontrada: {} para provincia {}", nombreQuery, repo.getProvincia());
                return 0;
            }

            QueryStorage queryStorage = queryOpt.get();

            /**
             * ES mejor usar el count real porque es maala idea a la larga no estimar la cantidad de registros
             * que podes tener :)
             */

            String queryOriginal = queryStorage.getSqlQuery();
            String queryConteo = construirQueryConteo(queryOriginal);
            Integer conteoReal = repo.ejecutarQueryConteoDesdeSQL(queryConteo);

            log.info("🔍 Conteo REAL para {}: {} registros (estimación previa: {})",
                    repo.getProvincia(),
                    conteoReal,
                    queryStorage.getRegistrosEstimados());

            return conteoReal != null ? conteoReal : 0;

        } catch (Exception e) {
            log.error("Error obteniendo conteo para {} - {}: {}",
                    repo.getProvincia(), nombreQuery, e.getMessage());
            return 0;
        }
    }

    private String construirQueryConteo(String queryOriginal) {
        // Remover ORDER BY si existe (no necesario para contar)
        String querySinOrder = queryOriginal.replaceAll("(?i)ORDER BY[^;]*", "").trim();

        // Envolver en subquery
        return String.format("SELECT COUNT(*) as total FROM (%s) AS subquery", querySinOrder);
    }

    /**
     * Limpia la query removiendo condiciones WHERE incompletas
     * @param query Query original
     * @return Query limpia y válida
     */
    private String limpiarQueryParaCount(String query) {
        // Remover líneas WHERE sin condición (ej: "WHERE campo_solo\n AND (...)")
        String limpia = query.replaceAll(
                "WHERE\\s+[\\w.]+\\s+(?=AND|OR|GROUP|ORDER|LIMIT|$)",
                "WHERE 1=1 "
        );

        // Remover LIMITs para count completo (opcional)
        limpia = limpia.replaceAll("LIMIT\\s+[^;]+", "");

        return limpia;
    }

    /**
     * Convierte una query SELECT en su equivalente COUNT(*).
     *
     * Transformación:
     * SELECT campo1, campo2 FROM tabla WHERE condicion ORDER BY campo
     * →
     * SELECT COUNT(*) FROM tabla WHERE condicion
     *
     * @param query Query original
     * @return Query COUNT(*)
     */
    private String convertirACount(String query) {
        // Pattern para detectar FROM hasta el final
        Pattern fromPattern = Pattern.compile(
                "FROM\\s+",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = fromPattern.matcher(query);

        if (!matcher.find()) {
            throw new IllegalArgumentException("Query sin cláusula FROM");
        }

        int posicionFrom = matcher.start();

        // Extraer desde FROM en adelante
        String desdeFrom = query.substring(posicionFrom);

        // Remover ORDER BY, LIMIT (no afectan COUNT)
        desdeFrom = desdeFrom
                .replaceAll("(?i)ORDER\\s+BY\\s+[^;]+", "")
                .replaceAll("(?i)LIMIT\\s+\\d+", "")
                .trim();

        // Construir query COUNT
        return "SELECT COUNT(*) " + desdeFrom;
    }

    /**
     * Decide la estrategia de consolidación según el volumen real de registros.
     *
     * Umbrales:
     * - < 10k: CRUDO en memoria
     * - 10k - 100k: CRUDO_STREAMING
     * - > 100k: FORZAR_AGREGACION
     *
     * @param analisisBase Análisis estático base
     * @param query Query original
     * @param cantidadRegistros Cantidad real de registros (COUNT(*))
     * @return Análisis con estrategia definitiva
     */
    private AnalisisConsolidacion decidirEstrategiaSegunVolumen(
            AnalisisConsolidacion analisisBase,
            String query,
            long cantidadRegistros) {

        TipoConsolidacion tipoFinal;
        String explicacion;

        if (cantidadRegistros < QueryAnalyzer.getUmbralCrudoStreaming()) {
            // CASO 1: Bajo volumen → Procesar crudo en memoria
            tipoFinal = TipoConsolidacion.CRUDO;
            explicacion = String.format(
                    "COUNT(*) = %,d registros - Bajo volumen, procesamiento CRUDO en memoria",
                    cantidadRegistros
            );
            log.info("✓ Estrategia: CRUDO en memoria ({} registros)", cantidadRegistros);

        } else if (cantidadRegistros < QueryAnalyzer.getUmbralCrudoForzarAgregacion()) {
            // CASO 2: Volumen medio → Streaming obligatorio
            tipoFinal = TipoConsolidacion.CRUDO_STREAMING;
            explicacion = String.format(
                    "COUNT(*) = %,d registros - Volumen medio, requiere STREAMING por chunks",
                    cantidadRegistros
            );
            log.warn("⚠ Estrategia: CRUDO_STREAMING ({} registros - sobre umbral de 10k)",
                    cantidadRegistros);

        } else {
            // CASO 3: Alto volumen → Forzar agregación
            tipoFinal = TipoConsolidacion.FORZAR_AGREGACION;
            explicacion = String.format(
                    "COUNT(*) = %,d registros - Volumen ALTO, se FORZARÁ agregación automática (GROUP BY)",
                    cantidadRegistros
            );
            log.error(" Estrategia: FORZAR_AGREGACION ({} registros - SUPERA umbral de 100k)",
                    cantidadRegistros);
        }

        // Retornar análisis actualizado
        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,                                   // esConsolidable
                tipoFinal,                              // tipo determinado dinámicamente
                (int) cantidadRegistros,                // registros reales (no estimados)
                1.0,                                    // confianza 100% (dato real)
                explicacion
        );
    }

    /**
     * Crea un análisis conservador cuando falla el COUNT(*).
     * Asume el peor caso: streaming obligatorio.
     *
     * @param analisisBase Análisis base
     * @return Análisis conservador con CRUDO_STREAMING
     */
    private AnalisisConsolidacion crearAnalisisConservador(AnalisisConsolidacion analisisBase) {
        log.warn("Creando análisis CONSERVADOR (falló COUNT(*)) - Asumiendo STREAMING");

        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,
                TipoConsolidacion.CRUDO_STREAMING,  // Estrategia conservadora
                null,
                0.0,
                "Error ejecutando COUNT(*) - Aplicando estrategia conservadora (STREAMING)"
        );
    }

    /**
     * Reescribe una query cruda agregando GROUP BY automático.
     *
     * Proceso:
     * 1. Detectar campos candidatos para GROUP BY (ubicación, tiempo, categorías)
     * 2. Detectar campos numéricos para SUM/COUNT
     * 3. Reescribir SELECT con agregaciones
     * 4. Agregar cláusula GROUP BY
     *
     * Ejemplo:
     * Original:
     *   SELECT provincia, mes, monto, cantidad
     *   FROM infracciones
     *   WHERE anio = 2024
     *
     * Reescrita:
     *   SELECT provincia, mes,
     *          SUM(monto) AS monto_total,
     *          SUM(cantidad) AS cantidad_total
     *   FROM infracciones
     *   WHERE anio = 2024
     *   GROUP BY provincia, mes
     *
     * @param query Query original sin GROUP BY
     * @param analisis Análisis de consolidación con campos identificados
     * @return Query reescrita con GROUP BY
     */
    public String reescribirConAgregacionAutomatica(
            String query,
            AnalisisConsolidacion analisis) {

        log.info("Reescribiendo query con agregación automática...");

        // Construir nuevo SELECT
        List<String> camposSelect = new ArrayList<>();

        // 1. Agregar campos de agrupación (sin modificar)
        camposSelect.addAll(analisis.getCamposAgrupacion());

        // 2. Agregar campos numéricos con SUM()
        for (String campoNumerico : analisis.getCamposNumericos()) {
            camposSelect.add(String.format("SUM(%s) AS %s_total", campoNumerico, campoNumerico));
        }

        // 3. Agregar COUNT(*) siempre
        camposSelect.add("COUNT(*) AS cantidad_registros");

        String nuevoSelect = String.join(", ", camposSelect);

        // Extraer FROM en adelante
        Pattern fromPattern = Pattern.compile("FROM\\s+", Pattern.CASE_INSENSITIVE);
        Matcher matcher = fromPattern.matcher(query);

        if (!matcher.find()) {
            throw new IllegalArgumentException("Query sin FROM");
        }

        String desdeFrom = query.substring(matcher.start());

        // Remover ORDER BY, LIMIT (incompatibles con GROUP BY sin estar en SELECT)
        desdeFrom = desdeFrom
                .replaceAll("(?i)ORDER\\s+BY\\s+[^;]+", "")
                .replaceAll("(?i)LIMIT\\s+\\d+", "")
                .trim();

        // Construir GROUP BY
        String groupBy = "GROUP BY " + String.join(", ", analisis.getCamposAgrupacion());

        // Query final
        String queryReescrita = String.format(
                "SELECT %s %s %s",
                nuevoSelect,
                desdeFrom,
                groupBy
        );

        log.info("Query reescrita exitosamente");
        log.debug("Query original: {}", query);
        log.debug("Query reescrita: {}", queryReescrita);

        return queryReescrita;
    }
}