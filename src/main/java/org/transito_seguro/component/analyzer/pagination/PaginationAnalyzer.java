package org.transito_seguro.component.analyzer.pagination;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.analyzer.query.SqlParser;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Orquestador principal para análisis de estrategias de paginación.
 *
 * RESPONSABILIDAD ÚNICA: Coordinar el análisis delegando a componentes especializados.
 *
 * Delega a:
 * - {@link SqlParser}: Validación y parseo de SQL
 * - {@link KeysetFieldDetector}: Detección de campos para keyset
 * - {@link PaginationStrategySelector}: Selección de estrategia óptima
 *
 * @author Transito Seguro Team
 * @version 3.0 - Refactorizado siguiendo Single Responsibility Principle
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaginationAnalyzer {

    private final SqlParser sqlParser;
    private final KeysetFieldDetector keysetFieldDetector;
    private final PaginationStrategySelector strategySelector;

    // ========== CONSTANTES ==========

    private static final int MAX_SQL_LENGTH = 100_000;

    /**
     * Pattern para detectar funciones de agregación.
     */
    private static final Pattern AGREGACION_PATTERN = Pattern.compile(
            "\\b(COUNT|SUM|AVG|MIN|MAX)\\s*\\(",
            Pattern.CASE_INSENSITIVE
    );

    // ========== MÉTODO PRINCIPAL ==========

    /**
     * Determina la estrategia de paginación óptima para una query SQL.
     *
     * @param sql Query SQL a analizar
     * @return Análisis con la estrategia recomendada y campos detectados
     * @throws IllegalArgumentException si el SQL es inválido o demasiado largo
     */
    public AnalisisPaginacion determinarEstrategia(String sql) {
        log.debug("Analizando estrategia de paginación para query");

        // Validación
        validarSQL(sql);

        // 1. Si es consolidada (GROUP BY), analizar keyset basado en GROUP BY
        if (esQueryConsolidada(sql)) {
            return analizarQueryConsolidada(sql);
        }

        // 2. Detectar ID de infracciones y su alias
        String aliasIdInfracciones = keysetFieldDetector.detectarAliasIdInfracciones(sql);
        boolean tieneIdInfracciones = aliasIdInfracciones != null;

        // 3. Detectar campos disponibles en el SELECT
        List<CampoKeyset> camposDisponibles = keysetFieldDetector.detectarCamposDisponibles(sql);

        // 4. Determinar estrategia según disponibilidad (delegado a selector)
        return strategySelector.determinarEstrategia(
                tieneIdInfracciones,
                aliasIdInfracciones,
                camposDisponibles
        );
    }

    // ========== MÉTODOS PRIVADOS ==========

    /**
     * Valida que el SQL sea procesable.
     */
    private void validarSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL no puede ser nulo o vacío");
        }

        if (sql.length() > MAX_SQL_LENGTH) {
            throw new IllegalArgumentException(
                    String.format("SQL excede el límite máximo de %d caracteres", MAX_SQL_LENGTH)
            );
        }

        String upper = sql.toUpperCase();
        if (!upper.contains("SELECT") || !upper.contains("FROM")) {
            throw new IllegalArgumentException("SQL debe contener SELECT...FROM");
        }
    }

    /**
     * Detecta si la query es consolidada (tiene GROUP BY con agregaciones).
     */
    private boolean esQueryConsolidada(String sql) {
        String sqlLimpio = sqlParser.limpiarComentariosSQL(sql);

        boolean tieneGroupBy = sqlParser.tieneGroupBy(sqlLimpio);
        boolean tieneAgg = AGREGACION_PATTERN.matcher(sqlLimpio).find();

        log.debug("Query consolidada: GROUP BY={}, Agregación={}", tieneGroupBy, tieneAgg);
        return tieneGroupBy && tieneAgg;
    }

    /**
     * Analiza una query consolidada (con GROUP BY).
     */
    private AnalisisPaginacion analizarQueryConsolidada(String sql) {
        List<CampoKeyset> camposGroupBy = keysetFieldDetector.extraerCamposGroupBy(sql);

        return strategySelector.determinarEstrategiaConsolidada(camposGroupBy);
    }
}