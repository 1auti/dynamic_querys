package org.transito_seguro.exception;

import lombok.Getter;
import org.transito_seguro.utils.SQLFormatter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Excepción personalizada para errores SQL con contexto completo
 */
@Getter
public class SQLExecutionException extends RuntimeException {
    private final String sqlCompleto;
    private final String errorOriginal;
    private final Integer posicionError;
    private final String queryName;
    private final String contexto;
    private final LocalDateTime timestamp;
    private final String tipoError;
    private final String codigoError;

    public SQLExecutionException(String message, String sqlCompleto, String errorOriginal,
                                 Integer posicionError, String queryName, String contexto,
                                 String tipoError, String codigoError) {
        super(message);
        this.sqlCompleto = sqlCompleto;
        this.errorOriginal = errorOriginal;
        this.posicionError = posicionError;
        this.queryName = queryName;
        this.contexto = contexto;
        this.tipoError = tipoError;
        this.codigoError = codigoError;
        this.timestamp = LocalDateTime.now();
    }

    public SQLExecutionException(String message, String sqlCompleto, String errorOriginal,
                                 Integer posicionError, String queryName, String contexto,
                                 String tipoError, String codigoError, Throwable cause) {
        super(message, cause);
        this.sqlCompleto = sqlCompleto;
        this.errorOriginal = errorOriginal;
        this.posicionError = posicionError;
        this.queryName = queryName;
        this.contexto = contexto;
        this.tipoError = tipoError;
        this.codigoError = codigoError;
        this.timestamp = LocalDateTime.now();
    }

    @Override
    public String getMessage() {
        return generarMensajeFormateado(false);
    }

    public String getMessageDetallado() {
        return generarMensajeFormateado(true);
    }

    public String getMessageResumido() {
        return generarMensajeFormateado(false);
    }

    private String generarMensajeFormateado(boolean detallado) {
        StringBuilder sb = new StringBuilder();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        sb.append("\n╔════════════════════════════════════════════════════════════════════════════════════════╗\n");
        sb.append("║ ERROR EN EJECUCIÓN DE SQL - ").append(timestamp.format(formatter)).append("\n");
        sb.append("╠════════════════════════════════════════════════════════════════════════════════════════╣\n");

        if (tipoError != null) {
            sb.append("║ Tipo: ").append(tipoError).append("\n");
        }

        if (codigoError != null) {
            sb.append("║ Código: ").append(codigoError).append("\n");
        }

        if (queryName != null) {
            sb.append("║ Query: ").append(queryName).append("\n");
        }

        if (contexto != null) {
            sb.append("║ Contexto: ").append(contexto).append("\n");
        }

        sb.append("║\n");
        sb.append("║ ERROR ORIGINAL:\n");
        sb.append("║ ").append(errorOriginal != null ? errorOriginal : "No disponible").append("\n");

        if (posicionError != null && detallado) {
            sb.append("║ Posición del error: ").append(posicionError).append("\n");

            // Mostrar línea y columna aproximada
            if (sqlCompleto != null) {
                int[] posicion = calcularLineaColumna(sqlCompleto, posicionError);
                sb.append("║ Línea: ").append(posicion[0]).append(", Columna: ").append(posicion[1]).append("\n");
            }
        }

        if (detallado && sqlCompleto != null) {
            sb.append("║\n");
            sb.append("║ SQL COMPLETO:\n");
            sb.append("╠════════════════════════════════════════════════════════════════════════════════════════╣\n");

            String sqlFormateado = formatearSQLConError(sqlCompleto, posicionError);
            String[] lineas = sqlFormateado.split("\n");
            for (String linea : lineas) {
                sb.append("║ ").append(linea).append("\n");
            }
        }

        sb.append("╚════════════════════════════════════════════════════════════════════════════════════════╝\n");

        return sb.toString();
    }

    private int[] calcularLineaColumna(String sql, int posicion) {
        if (posicion <= 0 || posicion > sql.length()) {
            return new int[]{-1, -1};
        }

        String subSql = sql.substring(0, posicion - 1);
        int linea = 1;
        int columna = 1;

        for (char c : subSql.toCharArray()) {
            if (c == '\n') {
                linea++;
                columna = 1;
            } else {
                columna++;
            }
        }

        return new int[]{linea, columna};
    }

    private String formatearSQLConError(String sql, Integer posicionError) {
        if (posicionError == null || posicionError <= 0 || posicionError > sql.length()) {
            return SQLFormatter.formatearSQL(sql);
        }

        StringBuilder sb = new StringBuilder();
        String[] lineas = sql.split("\n");
        int posicionAcumulada = 0;
        boolean errorMarcado = false;

        for (int i = 0; i < lineas.length; i++) {
            String linea = lineas[i];
            int longitudLinea = linea.length() + 1; // +1 por el \n

            if (!errorMarcado && posicionError >= posicionAcumulada &&
                    posicionError <= posicionAcumulada + longitudLinea) {
                int columnaError = posicionError - posicionAcumulada;

                // Agregar línea con marcador de error
                sb.append(linea).append("\n");

                // Crear marcador de posición (sin String.repeat)
                String marcador = repeatString(" ", Math.max(0, columnaError - 1)) + "^\n";
                sb.append(marcador);
                sb.append(repeatString(" ", Math.max(0, columnaError - 1))).append("└── ERROR aquí\n");
                errorMarcado = true;
            } else {
                sb.append(linea).append("\n");
            }

            posicionAcumulada += longitudLinea;
        }

        return sb.toString();
    }

    private String repeatString(String str, int count) {
        if (count <= 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}