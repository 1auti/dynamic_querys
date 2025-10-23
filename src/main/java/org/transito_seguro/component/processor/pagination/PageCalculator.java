package org.transito_seguro.component.processor.pagination;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.Map;

/**
 * Calculador de tamaños de página y offsets para paginación.
 *
 * Responsabilidades:
 * - Calcular tamaño de página óptimo según memoria
 * - Construir filtros para paginación OFFSET
 * - Construir filtros para paginación KEYSET
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PageCalculator {

    private final MemoryMonitor memoryMonitor;
    private final KeysetManager keysetManager;

    /**
     * Tamaño de página por defecto.
     */
    private static final int DEFAULT_PAGE_SIZE = 10000;

    /**
     * Tamaño mínimo de página (seguridad).
     */
    private static final int MIN_PAGE_SIZE = 1000;

    /**
     * Calcula el tamaño de página óptimo según memoria disponible.
     *
     * @param batchSizeBase Tamaño base deseado
     * @return Tamaño ajustado según memoria
     */
    public int calcularTamanioPaginaOptimo(int batchSizeBase) {
        // Si el tamaño base es muy pequeño, usar default
        if (batchSizeBase < MIN_PAGE_SIZE) {
            batchSizeBase = DEFAULT_PAGE_SIZE;
        }

        // Ajustar según memoria disponible (delegado a MemoryMonitor)
        int tamanoAjustado = memoryMonitor.calcularTamanoLoteOptimo(batchSizeBase);

        log.debug("📏 Tamaño de página calculado: {} (base: {})",
                tamanoAjustado, batchSizeBase);

        return tamanoAjustado;
    }

    /**
     * Crea filtros para la primera iteración de paginación.
     *
     * Primera iteración:
     * - NO usa keyset
     * - Usa límite razonable (no Integer.MAX_VALUE)
     * - offset = 0
     *
     * @param filtrosOriginales Filtros base
     * @param batchSize Tamaño de página
     * @return Filtros para primera página
     */
    public ParametrosFiltrosDTO crearFiltrosPrimeraPagina(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize) {

        log.debug("📄 Creando filtros para primera página: limite={}", batchSize);

        return filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(null)
                .lastId(null)
                .lastSerieEquipo(null)
                .lastLugar(null)
                .lastKeysetConsolidacion(null)
                .build();
    }

    /**
     * Crea filtros para paginación OFFSET (sin keyset).
     *
     * Usa offset incremental: 0, 10000, 20000, etc.
     *
     * @param filtrosOriginales Filtros base
     * @param batchSize Tamaño de página
     * @param offset Offset actual
     * @return Filtros con offset
     */
    public ParametrosFiltrosDTO crearFiltrosConOffset(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            int offset) {

        log.debug("📄 Creando filtros con OFFSET: limite={}, offset={}",
                batchSize, offset);

        return filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(offset)
                .lastId(null)
                .lastSerieEquipo(null)
                .lastLugar(null)
                .lastKeysetConsolidacion(null)
                .build();
    }

    /**
     * Crea filtros para paginación KEYSET.
     *
     * Usa el último keyset guardado para la provincia.
     * Soporta dos tipos:
     * - Keyset estándar (id, serie_equipo, lugar)
     * - Keyset consolidación (campo_0, campo_1, campo_2)
     *
     * @param filtrosOriginales Filtros base
     * @param batchSize Tamaño de página
     * @param provincia Nombre de la provincia
     * @return Filtros con keyset
     */
    public ParametrosFiltrosDTO crearFiltrosConKeyset(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            String provincia) {

        // Verificar si existe keyset para esta provincia
        if (!keysetManager.existeKeyset(provincia)) {
            log.warn("⚠️ No existe keyset para {}, usando primera página", provincia);
            return crearFiltrosPrimeraPagina(filtrosOriginales, batchSize);
        }

        Object[] keyset = keysetManager.obtenerKeyset(provincia);

        // Detectar tipo de keyset
        if (keysetManager.esKeysetEstandar(keyset)) {
            return crearFiltrosConKeysetEstandar(filtrosOriginales, batchSize, keyset, provincia);
        } else {
            return crearFiltrosConKeysetConsolidacion(filtrosOriginales, batchSize, keyset, provincia);
        }
    }

    /**
     * Crea filtros con keyset estándar (id, serie_equipo, lugar).
     */
    private ParametrosFiltrosDTO crearFiltrosConKeysetEstandar(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            Object[] keyset,
            String provincia) {

        log.debug("🔑 Keyset estándar para {}: id={}", provincia, keyset[0]);

        return filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(null)  // NUNCA usar offset con keyset
                .lastId((Integer) keyset[0])
                .lastSerieEquipo(keyset.length > 1 ? (String) keyset[1] : null)
                .lastLugar(keyset.length > 2 ? (String) keyset[2] : null)
                .lastKeysetConsolidacion(null)
                .build();
    }

    /**
     * Crea filtros con keyset de consolidación (campo_0, campo_1, campo_2).
     */
    private ParametrosFiltrosDTO crearFiltrosConKeysetConsolidacion(
            ParametrosFiltrosDTO filtrosOriginales,
            int batchSize,
            Object[] keyset,
            String provincia) {

        Map<String, Object> keysetMap = keysetManager.construirKeysetMap(keyset);

        log.debug("🔑 Keyset consolidación para {}: {} campos", provincia, keysetMap.size());

        return filtrosOriginales.toBuilder()
                .limite(batchSize)
                .offset(null)
                .lastId(null)
                .lastSerieEquipo(null)
                .lastLugar(null)
                .lastKeysetConsolidacion(keysetMap)
                .build();
    }

    /**
     * Determina si se debe continuar paginando según el tamaño del lote.
     *
     * Si el lote está incompleto (< batchSize), es la última página.
     *
     * @param tamanoLote Cantidad de registros en el lote actual
     * @param batchSize Tamaño de página esperado
     * @return true si debe continuar paginando
     */
    public boolean debeContinuarPaginando(int tamanoLote, int batchSize) {
        boolean continuar = tamanoLote >= batchSize;

        if (!continuar) {
            log.debug("🏁 Última página detectada: {} < {}", tamanoLote, batchSize);
        }

        return continuar;
    }

    /**
     * Calcula el progreso de paginación.
     *
     * @param procesados Registros procesados hasta ahora
     * @param estimacion Estimación total (puede ser null)
     * @return Porcentaje de progreso (0-100) o -1 si no hay estimación
     */
    public double calcularProgreso(int procesados, Integer estimacion) {
        if (estimacion == null || estimacion == 0) {
            return -1.0;
        }

        return (double) procesados / estimacion * 100;
    }
}