package org.transito_seguro.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

/**
 * Utilidad mejorada para detectar tipos de campos SQL y funciones de agregación
 */
@Slf4j
public final class SqlFieldDetector {

    private SqlFieldDetector() {}

    // Patrones para funciones SQL
    private static final Pattern COUNT_PATTERN = Pattern.compile("count\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SUM_PATTERN = Pattern.compile("sum\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern AVG_PATTERN = Pattern.compile("avg\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern MAX_PATTERN = Pattern.compile("max\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern MIN_PATTERN = Pattern.compile("min\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);

    /**
     * Detecta si un campo es numérico basado en su nombre o función SQL
     */
    public static boolean esNumericoPorNombre(String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return false;
        }

        String campoLimpio = campo.toLowerCase().trim();

        // 1. Detectar funciones de agregación SQL
        if (esFuncionAgregacion(campoLimpio)) {
            log.debug("Campo detectado como función de agregación: {}", campo);
            return true;
        }

        // 2. Detectar por nombres comunes
        if (esNombreNumerico(campoLimpio)) {
            log.debug("Campo detectado como numérico por nombre: {}", campo);
            return true;
        }

        // 3. Detectar patrones específicos
        if (esPatronNumerico(campoLimpio)) {
            log.debug("Campo detectado como numérico por patrón: {}", campo);
            return true;
        }

        return false;
    }

    /**
     * Detecta funciones de agregación SQL
     */
    private static boolean esFuncionAgregacion(String campo) {
        return COUNT_PATTERN.matcher(campo).matches() ||
                SUM_PATTERN.matcher(campo).matches() ||
                AVG_PATTERN.matcher(campo).matches() ||
                MAX_PATTERN.matcher(campo).matches() ||
                MIN_PATTERN.matcher(campo).matches();
    }

    /**
     * Detecta nombres comunes de campos numéricos
     */
    private static boolean esNombreNumerico(String campo) {
        return campo.equals("count") ||
                campo.equals("total") ||
                campo.equals("cantidad") ||
                campo.equals("numero") ||
                campo.equals("counter") ||
                campo.contains("total") ||
                campo.contains("cantidad") ||
                campo.contains("counter") ||
                campo.contains("numero") ||
                campo.contains("importe") ||
                campo.contains("monto") ||
                campo.contains("precio") ||
                campo.contains("valor");
    }

    /**
     * Detecta patrones específicos de campos numéricos
     */
    private static boolean esPatronNumerico(String campo) {
        return campo.endsWith("_count") ||
                campo.endsWith("_total") ||
                campo.endsWith("_cantidad") ||
                campo.endsWith("_num") ||
                campo.startsWith("num_") ||
                campo.startsWith("total_") ||
                campo.startsWith("count_");
    }

    /**
     * Detecta si un campo es geográfico
     */
    public static boolean esGeografico(String campo) {
        if (campo == null) return false;

        String campoLower = campo.toLowerCase();
        return campoLower.contains("provincia") ||
                campoLower.contains("municipio") ||
                campoLower.contains("lugar") ||
                campoLower.contains("contexto") ||
                campoLower.contains("partido") ||
                campoLower.contains("localidad") ||
                campoLower.contains("ciudad") ||
                campoLower.contains("region");
    }

    /**
     * Detecta si un campo es categórico
     */
    public static boolean esCategorico(String campo) {
        if (campo == null) return false;

        String campoLower = campo.toLowerCase();
        return campoLower.contains("descripcion") ||
                campoLower.contains("estado") ||
                campoLower.contains("tipo") ||
                campoLower.contains("categoria") ||
                campoLower.contains("serie") ||
                campoLower.contains("equipo") ||
                campoLower.contains("clase") ||
                campoLower.contains("grupo");
    }

    /**
     * Detecta si un campo es temporal
     */
    public static boolean esTemporal(String campo) {
        if (campo == null) return false;

        String campoLower = campo.toLowerCase();
        return campoLower.contains("fecha") ||
                campoLower.contains("time") ||
                campoLower.contains("timestamp") ||
                campoLower.contains("hora") ||
                campoLower.contains("mes") ||
                campoLower.contains("año") ||
                campoLower.contains("dia");
    }

    /**
     * Normaliza nombres de funciones SQL para comparación
     */
    public static String normalizarNombreFuncion(String campo) {
        if (campo == null) return "";

        return campo.toLowerCase()
                .replaceAll("\\s+", "")
                .trim();
    }

    /**
     * Extrae el nombre de la función SQL sin parámetros
     */
    public static String extraerNombreFuncion(String campo) {
        if (campo == null) return "";

        String normalizado = normalizarNombreFuncion(campo);

        if (normalizado.startsWith("count(")) return "count";
        if (normalizado.startsWith("sum(")) return "sum";
        if (normalizado.startsWith("avg(")) return "avg";
        if (normalizado.startsWith("max(")) return "max";
        if (normalizado.startsWith("min(")) return "min";

        return campo;
    }

    /**
     * Determina la prioridad de un campo para consolidación
     * Menor número = mayor prioridad
     */
    public static int obtenerPrioridadConsolidacion(String campo) {
        if (esNumericoPorNombre(campo)) return 1;      // Campos numéricos tienen prioridad baja (se suman)
        if (esGeografico(campo)) return 2;             // Campos geográficos prioridad media
        if (esCategorico(campo)) return 3;             // Campos categóricos prioridad alta
        if (esTemporal(campo)) return 4;               // Campos temporales prioridad muy alta
        return 5;                                      // Otros campos prioridad más baja
    }
}