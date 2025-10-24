package org.transito_seguro.component.builder.sql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Componente especializado en limpieza y normalización de SQL.
 *
 * Responsabilidades:
 * - Remover comentarios (línea y bloque)
 * - Normalizar espacios en blanco
 * - Remover terminadores temporalmente
 * - Validar sintaxis básica
 */
@Slf4j
@Component
public class SqlCleaner {

    /**
     * Limpia comentarios y normaliza espacios en blanco del SQL.
     *
     * @param sql Query SQL
     * @return SQL limpio
     */
    public String limpiarComentarios(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        return sql.replaceAll("--[^\n\r]*", "")      // Comentarios de línea
                .replaceAll("/\\*.*?\\*/", "")        // Comentarios de bloque
                .replaceAll("\\s+", " ")               // Normalizar espacios
                .trim();
    }

    /**
     * Remueve punto y coma final temporalmente.
     *
     * @param sql Query SQL
     * @return SQL sin terminador
     */
    public String removerTerminadores(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        return sql.replaceAll(";\\s*$", "").trim();
    }

    /**
     * Limpieza completa (comentarios + terminadores).
     *
     * @param sql Query SQL
     * @return SQL completamente limpio
     */
    public String limpiar(String sql) {
        String limpio = limpiarComentarios(sql);
        limpio = removerTerminadores(limpio);

        log.debug("SQL limpiado: {} caracteres", limpio.length());
        return limpio;
    }

    /**
     * Valida que el SQL tenga estructura básica válida.
     *
     * @param sql Query SQL
     * @throws IllegalArgumentException si el SQL es inválido
     */
    public void validarEstructuraBasica(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL no puede estar vacío");
        }

        String upper = sql.toUpperCase();

        if (!upper.contains("SELECT")) {
            throw new IllegalArgumentException("SQL debe contener SELECT");
        }

        if (!upper.contains("FROM")) {
            throw new IllegalArgumentException("SQL debe contener FROM");
        }

        // Validar balance de paréntesis
        long aperturas = sql.chars().filter(ch -> ch == '(').count();
        long cierres = sql.chars().filter(ch -> ch == ')').count();

        if (aperturas != cierres) {
            throw new IllegalArgumentException(
                    String.format("Paréntesis desbalanceados: %d aperturas, %d cierres",
                            aperturas, cierres));
        }
    }

    /**
     * Normaliza espacios en blanco excesivos.
     *
     * @param sql Query SQL
     * @return SQL con espacios normalizados
     */
    public String normalizarEspacios(String sql) {
        if (sql == null) {
            return null;
        }

        return sql.replaceAll("\\s+", " ").trim();
    }
}