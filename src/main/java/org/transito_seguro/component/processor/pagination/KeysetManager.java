package org.transito_seguro.component.processor.pagination;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Gestor de keyset para paginación avanzada.
 *
 * Responsabilidades:
 * - Guardar último keyset por provincia
 * - Detectar tipo de keyset (estándar vs consolidación)
 * - Construir mapas de keyset para queries
 */
@Slf4j
@Component
public class KeysetManager {

    /**
     * Mapa con el último keyset por provincia.
     * Key: provincia, Value: array de valores del keyset
     */
    private final Map<String, Object[]> lastKeyPerProvince = new ConcurrentHashMap<>();

    /**
     * Limpia todos los keysets almacenados.
     */
    public void limpiar() {
        lastKeyPerProvince.clear();
        log.debug("🗑️ Keysets limpiados para todas las provincias");
    }

    /**
     * Limpia el keyset de una provincia específica.
     *
     * @param provincia Nombre de la provincia
     */
    public void limpiarProvincia(String provincia) {
        lastKeyPerProvince.remove(provincia);
        log.debug("🗑️ Keyset limpiado para provincia: {}", provincia);
    }

    /**
     * Verifica si existe keyset guardado para una provincia.
     *
     * @param provincia Nombre de la provincia
     * @return true si existe keyset guardado
     */
    public boolean existeKeyset(String provincia) {
        return lastKeyPerProvince.containsKey(provincia);
    }

    /**
     * Obtiene el keyset guardado para una provincia.
     *
     * @param provincia Nombre de la provincia
     * @return Array de valores del keyset o null si no existe
     */
    public Object[] obtenerKeyset(String provincia) {
        return lastKeyPerProvince.get(provincia);
    }

    /**
     * Guarda el keyset del último registro de un lote.
     *
     * Detecta automáticamente si es:
     * - Keyset estándar (con campo 'id')
     * - Keyset de consolidación (primeros 3 campos disponibles)
     *
     * @param lote Lista de registros del lote
     * @param provincia Nombre de la provincia
     */
    public void guardarKeyset(List<Map<String, Object>> lote, String provincia) {
        if (lote == null || lote.isEmpty()) {
            log.debug("⚠️ Lote vacío, no se puede guardar keyset para {}", provincia);
            return;
        }

        try {
            Map<String, Object> ultimoRegistro = lote.get(lote.size() - 1);

            // CASO 1: Query estándar con campo 'id'
            if (ultimoRegistro.containsKey("id") && ultimoRegistro.get("id") != null) {
                guardarKeysetEstandar(ultimoRegistro, provincia);
            }
            // CASO 2: Query de consolidación SIN campo 'id'
            else {
                guardarKeysetConsolidacion(ultimoRegistro, provincia);
            }

        } catch (Exception e) {
            log.warn("⚠️ Error guardando keyset para {}: {}", provincia, e.getMessage());
        }
    }

    /**
     * Guarda keyset estándar (id, serie_equipo, lugar).
     */
    private void guardarKeysetEstandar(Map<String, Object> ultimoRegistro, String provincia) {
        Object id = ultimoRegistro.get("id");

        lastKeyPerProvince.put(provincia, new Object[]{
                id,
                ultimoRegistro.get("serie_equipo"),
                ultimoRegistro.get("lugar")
        });

        log.debug("🔑 Keyset estándar guardado para {}: id={} (tipo: {})",
                provincia, id, id.getClass().getSimpleName());
    }

    /**
     * Guarda keyset de consolidación (primeros 3 campos disponibles).
     */
    private void guardarKeysetConsolidacion(Map<String, Object> ultimoRegistro, String provincia) {
        List<Object> keyValues = new ArrayList<>();
        int count = 0;

        // Tomar primeros 3 campos con valor no nulo
        for (Map.Entry<String, Object> entry : ultimoRegistro.entrySet()) {
            if (entry.getValue() != null && count < 3) {
                keyValues.add(entry.getValue());
                count++;
            }
        }

        if (!keyValues.isEmpty()) {
            lastKeyPerProvince.put(provincia, keyValues.toArray());

            log.debug("🔑 Keyset consolidación guardado para {}: {} campos (tipos: {})",
                    provincia,
                    keyValues.size(),
                    keyValues.stream()
                            .map(v -> v.getClass().getSimpleName())
                            .collect(Collectors.joining(", ")));
        } else {
            log.warn("⚠️ No se encontraron campos válidos para keyset en {}", provincia);
        }
    }

    /**
     * Determina si un keyset es de tipo estándar (con ID numérico).
     *
     * @param keyset Array de valores del keyset
     * @return true si es keyset estándar
     */
    public boolean esKeysetEstandar(Object[] keyset) {
        return keyset != null
                && keyset.length >= 1
                && keyset[0] instanceof Integer;
    }

    /**
     * Construye un mapa de keyset para queries de consolidación.
     *
     * Genera claves genéricas: campo_0, campo_1, campo_2
     *
     * @param keyset Array de valores del keyset
     * @return Mapa con claves genéricas
     */
    public Map<String, Object> construirKeysetMap(Object[] keyset) {
        Map<String, Object> keysetMap = new HashMap<>();

        if (keyset == null) {
            return keysetMap;
        }

        for (int i = 0; i < Math.min(keyset.length, 3); i++) {
            if (keyset[i] != null) {
                keysetMap.put("campo_" + i, keyset[i]);
            }
        }

        log.debug("🗺️ Keyset map construido: {} campos", keysetMap.size());
        return keysetMap;
    }

    /**
     * Obtiene estadísticas de keysets almacenados.
     *
     * @return Mapa con provincia y cantidad de campos en su keyset
     */
    public Map<String, Integer> obtenerEstadisticas() {
        Map<String, Integer> stats = new HashMap<>();

        lastKeyPerProvince.forEach((provincia, keyset) -> {
            int cantidadCampos = keyset != null ? keyset.length : 0;
            stats.put(provincia, cantidadCampos);
        });

        return stats;
    }

    /**
     * Obtiene cantidad de provincias con keyset guardado.
     *
     * @return Número de provincias
     */
    public int cantidadProvinciasConKeyset() {
        return lastKeyPerProvince.size();
    }
}