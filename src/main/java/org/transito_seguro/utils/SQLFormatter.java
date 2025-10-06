package org.transito_seguro.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SQLFormatter {

    /**
     * Formatea SQL para mejor legibilidad en logs
     */
    public static String formatearSQL(String sql) {
        if (sql == null) return "SQL no disponible";

        return sql.replaceAll("\\s+", " ")
                .replaceAll("(?i) FROM ", "\nFROM ")
                .replaceAll("(?i) WHERE ", "\nWHERE ")
                .replaceAll("(?i) AND ", "\n  AND ")
                .replaceAll("(?i) OR ", "\n   OR ")
                .replaceAll("(?i) GROUP BY ", "\nGROUP BY ")
                .replaceAll("(?i) ORDER BY ", "\nORDER BY ")
                .replaceAll("(?i) LIMIT ", "\nLIMIT ")
                .replaceAll("(?i) OFFSET ", "\nOFFSET ")
                .replaceAll("(?i) HAVING ", "\nHAVING ")
                .replaceAll("(?i) UNION ", "\nUNION\n")
                .replaceAll("(?i) JOIN ", "\nJOIN ")
                .replaceAll("(?i) ON ", "\n  ON ");
    }

    /**
     * Acorta SQL para logging (útil para queries muy largos)
     */
    public static String acortarSQL(String sql, int maxLength) {
        if (sql == null) return "SQL no disponible";
        if (sql.length() <= maxLength) return sql;

        return sql.substring(0, maxLength) + "... [TRUNCADO, longitud original: " + sql.length() + " caracteres]";
    }

    /**
     * Método alternativo para String.repeat en Java 8
     */
    public static String repeatString(String str, int count) {
        if (count <= 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}