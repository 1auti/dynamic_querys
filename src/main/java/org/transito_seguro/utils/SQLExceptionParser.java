package org.transito_seguro.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.transito_seguro.exception.SQLExecutionException;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@UtilityClass
public class SQLExceptionParser {

    // Patrones regex para extraer información de errores de diferentes bases de datos
    private static final Pattern PATTERN_POSICION = Pattern.compile("position:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_LINEA = Pattern.compile("line\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_CODIGO_SQL = Pattern.compile("SQLSTATE\\s*[=:]\\s*([A-Z0-9]+)", Pattern.CASE_INSENSITIVE);

    // Patrones para errores específicos
    private static final Pattern PATTERN_GROUP_BY_ERROR = Pattern.compile("group by", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_SYNTAX_ERROR = Pattern.compile("syntax", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_COLUMN_NOT_FOUND = Pattern.compile("column.*not found", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_TABLE_NOT_FOUND = Pattern.compile("table.*not found", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DUPLICATE_COLUMN = Pattern.compile("duplicate column", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_UNIQUE_VIOLATION = Pattern.compile("unique.*violat", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_FOREIGN_KEY = Pattern.compile("foreign key", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_NOT_NULL = Pattern.compile("not null", Pattern.CASE_INSENSITIVE);

    /**
     * Extrae información detallada de una excepción SQL
     */
    public static SQLExecutionException parse(Exception e, String sqlCompleto, String queryName, String contexto) {
        String errorOriginal = e.getMessage();
        Integer posicionError = extraerPosicionError(e, errorOriginal);
        String tipoError = determinarTipoError(e, errorOriginal);
        String codigoError = extraerCodigoError(e, errorOriginal);

        String mensajeResumen = construirMensajeResumen(errorOriginal, queryName, contexto, tipoError);

        return new SQLExecutionException(
                mensajeResumen,
                sqlCompleto,
                errorOriginal,
                posicionError,
                queryName,
                contexto,
                tipoError,
                codigoError,
                e
        );
    }

    /**
     * Extrae la posición del error del mensaje
     */
    private static Integer extraerPosicionError(Exception e, String mensajeError) {
        if (mensajeError == null) return null;

        try {
            // Buscar en el mensaje de error
            Matcher matcher = PATTERN_POSICION.matcher(mensajeError);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(1));
            }

            // Para UncategorizedSQLException, puede contener información adicional
            if (e instanceof UncategorizedSQLException) {
                UncategorizedSQLException uncategorizedEx = (UncategorizedSQLException) e;
                String sqlExMessage = uncategorizedEx.getSQLException().getMessage();
                matcher = PATTERN_POSICION.matcher(sqlExMessage);
                if (matcher.find()) {
                    return Integer.parseInt(matcher.group(1));
                }
            }

        } catch (Exception ex) {
            log.debug("No se pudo extraer posición del error", ex);
        }
        return null;
    }

    /**
     * Extrae el código de error SQL
     */
    private static String extraerCodigoError(Exception e, String mensajeError) {
        if (mensajeError == null) return null;

        try {
            // Buscar código SQLSTATE en el mensaje
            Matcher matcher = PATTERN_CODIGO_SQL.matcher(mensajeError);
            if (matcher.find()) {
                return matcher.group(1);
            }

            // Para SQLException directa
            if (e instanceof SQLException) {
                SQLException sqlEx = (SQLException) e;
                return sqlEx.getSQLState();
            }

            // Para UncategorizedSQLException
            if (e instanceof UncategorizedSQLException) {
                UncategorizedSQLException uncategorizedEx = (UncategorizedSQLException) e;
                return uncategorizedEx.getSQLException().getSQLState();
            }

        } catch (Exception ex) {
            log.debug("No se pudo extraer código de error", ex);
        }
        return null;
    }

    /**
     * Determina el tipo de error basado en la excepción y el mensaje
     */
    private static String determinarTipoError(Exception e, String mensajeError) {
        if (mensajeError == null) return "Error Desconocido";

        String mensajeLower = mensajeError.toLowerCase();

        // Primero verificar por tipo de excepción
        if (e instanceof BadSqlGrammarException) {
            return "Error de Sintaxis SQL";
        } else if (e instanceof CannotGetJdbcConnectionException) {
            return "Error de Conexión a Base de Datos";
        } else if (e instanceof UncategorizedSQLException) {
            return "Error SQL No Categorizado";
        }

        // Luego verificar por patrones en el mensaje
        if (PATTERN_SYNTAX_ERROR.matcher(mensajeLower).find()) {
            return "Error de Sintaxis";
        } else if (PATTERN_GROUP_BY_ERROR.matcher(mensajeLower).find() &&
                mensajeLower.contains("aggregat")) {
            return "Error de Agregación en GROUP BY";
        } else if (PATTERN_COLUMN_NOT_FOUND.matcher(mensajeLower).find()) {
            return "Columna No Encontrada";
        } else if (PATTERN_TABLE_NOT_FOUND.matcher(mensajeLower).find()) {
            return "Tabla No Encontrada";
        } else if (PATTERN_DUPLICATE_COLUMN.matcher(mensajeLower).find()) {
            return "Columna Duplicada";
        } else if (PATTERN_UNIQUE_VIOLATION.matcher(mensajeLower).find()) {
            return "Violación de Restricción Única";
        } else if (PATTERN_FOREIGN_KEY.matcher(mensajeLower).find()) {
            return "Violación de Clave Foránea";
        } else if (PATTERN_NOT_NULL.matcher(mensajeLower).find() && mensajeLower.contains("violat")) {
            return "Violación de NOT NULL";
        } else if (mensajeLower.contains("timeout") || mensajeLower.contains("connection")) {
            return "Error de Conexión/Timeout";
        } else if (mensajeLower.contains("permission") || mensajeLower.contains("access denied")) {
            return "Error de Permisos";
        } else {
            return "Error de Base de Datos";
        }
    }

    /**
     * Construye un mensaje resumen del error
     */
    private static String construirMensajeResumen(String errorOriginal, String queryName, String contexto, String tipoError) {
        StringBuilder sb = new StringBuilder();

        sb.append(tipoError);

        if (queryName != null) {
            sb.append(" en query '").append(queryName).append("'");
        }

        if (contexto != null) {
            sb.append(" (").append(contexto).append(")");
        }

        sb.append(": ");

        if (errorOriginal != null) {
            String mensajeLimpio = limpiarMensajeError(errorOriginal);
            sb.append(mensajeLimpio);
        }

        return sb.toString();
    }

    /**
     * Limpia el mensaje de error para hacerlo más legible
     */
    private static String limpiarMensajeError(String mensaje) {
        if (mensaje == null) return "No hay detalles del error";

        // Tomar solo la primera línea para el resumen
        String[] lineas = mensaje.split("\n");
        String primeraLinea = lineas[0].trim();

        // Remover prefijos comunes
        return primeraLinea.replaceFirst("^ERROR:\\s*", "")
                .replaceFirst("^Exception:\\s*", "")
                .replaceFirst("^SQL\\s*[Ee]rror:\\s*", "")
                .trim();
    }

    // ==================== DETECCIÓN DE ERRORES ESPECÍFICOS ====================

    public static boolean esErrorGroupByAgregacion(Exception e) {
        String mensaje = e.getMessage();
        if (mensaje == null) return false;

        String mensajeLower = mensaje.toLowerCase();
        return (mensajeLower.contains("group by") &&
                (mensajeLower.contains("aggregat") ||
                        mensajeLower.contains("must appear in the group by clause") ||
                        mensajeLower.contains("no se permiten funciones de agregación")));
    }

    public static boolean esErrorSintaxis(Exception e) {
        return e instanceof BadSqlGrammarException ||
                (e.getMessage() != null &&
                        (e.getMessage().toLowerCase().contains("bad sql grammar") ||
                                e.getMessage().toLowerCase().contains("syntax error")));
    }

    public static boolean esErrorConexion(Exception e) {
        return e instanceof CannotGetJdbcConnectionException ||
                (e.getMessage() != null &&
                        (e.getMessage().toLowerCase().contains("connection") ||
                                e.getMessage().toLowerCase().contains("timeout") ||
                                e.getMessage().toLowerCase().contains("socket") ||
                                e.getMessage().toLowerCase().contains("cannot get jdbc connection")));
    }

    public static boolean esErrorPermisos(Exception e) {
        String mensaje = e.getMessage();
        if (mensaje == null) return false;

        String mensajeLower = mensaje.toLowerCase();
        return mensajeLower.contains("permission") ||
                mensajeLower.contains("access denied") ||
                mensajeLower.contains("not authorized") ||
                mensajeLower.contains("insufficient privileges");
    }

    public static boolean esErrorColumnaNoExiste(Exception e) {
        String mensaje = e.getMessage();
        if (mensaje == null) return false;

        String mensajeLower = mensaje.toLowerCase();
        return PATTERN_COLUMN_NOT_FOUND.matcher(mensajeLower).find() ||
                mensajeLower.contains("column") && mensajeLower.contains("not exist") ||
                mensajeLower.contains("invalid column name");
    }

    public static boolean esErrorTablaNoExiste(Exception e) {
        String mensaje = e.getMessage();
        if (mensaje == null) return false;

        String mensajeLower = mensaje.toLowerCase();
        return PATTERN_TABLE_NOT_FOUND.matcher(mensajeLower).find() ||
                mensajeLower.contains("table") && mensajeLower.contains("not exist") ||
                mensajeLower.contains("invalid object name") && mensajeLower.contains("table");
    }

    /**
     * Obtiene sugerencias para el error detectado (sin text blocks)
     */
    public static String obtenerSugerencias(Exception e, String sql) {
        if (esErrorGroupByAgregacion(e)) {
            return buildSugerenciaGroupBy();
        }

        if (esErrorSintaxis(e)) {
            return buildSugerenciaSintaxis();
        }

        if (esErrorConexion(e)) {
            return buildSugerenciaConexion();
        }

        if (esErrorColumnaNoExiste(e)) {
            return buildSugerenciaColumnaNoExiste();
        }

        if (esErrorTablaNoExiste(e)) {
            return buildSugerenciaTablaNoExiste();
        }

        return buildSugerenciaGeneral();
    }

    // Métodos auxiliares para construir sugerencias (sin text blocks)
    private static String buildSugerenciaGroupBy() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA: Error de función de agregación en GROUP BY\n");
        sb.append("- Verificar que las columnas en SELECT que no son funciones de agregación\n");
        sb.append("  estén incluidas en la cláusula GROUP BY\n");
        sb.append("- Ejemplo incorrecto: SELECT departamento, COUNT(*) FROM tabla\n");
        sb.append("- Ejemplo correcto:   SELECT departamento, COUNT(*) FROM tabla GROUP BY departamento\n");
        sb.append("- Las funciones como COUNT(), SUM(), AVG(), MAX(), MIN() no van en GROUP BY\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }

    private static String buildSugerenciaSintaxis() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA: Error de sintaxis SQL\n");
        sb.append("- Revisar puntos y comas faltantes\n");
        sb.append("- Verificar paréntesis balanceados\n");
        sb.append("- Confirmar que todas las palabras clave estén escritas correctamente\n");
        sb.append("- Validar nombres de tablas y columnas (respetar mayúsculas/minúsculas)\n");
        sb.append("- Revisar comillas en textos literales\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }

    private static String buildSugerenciaConexion() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA: Error de conexión a la base de datos\n");
        sb.append("- Verificar que el servidor de base de datos esté en ejecución\n");
        sb.append("- Confirmar credenciales de conexión (usuario/contraseña)\n");
        sb.append("- Revisar configuración de red y firewall\n");
        sb.append("- Verificar timeout de conexión en la configuración\n");
        sb.append("- Validar URL de conexión JDBC\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }

    private static String buildSugerenciaColumnaNoExiste() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA: Columna no encontrada\n");
        sb.append("- Verificar el nombre de la columna (respetar mayúsculas/minúsculas)\n");
        sb.append("- Confirmar que la columna existe en la tabla\n");
        sb.append("- Revisar aliases en consultas complejas\n");
        sb.append("- Validar que la tabla tenga las columnas referenciadas\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }

    private static String buildSugerenciaTablaNoExiste() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA: Tabla no encontrada\n");
        sb.append("- Verificar el nombre de la tabla (respetar mayúsculas/minúsculas)\n");
        sb.append("- Confirmar que la tabla existe en la base de datos\n");
        sb.append("- Revisar el esquema (schema) de la tabla\n");
        sb.append("- Validar permisos de acceso a la tabla\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }

    private static String buildSugerenciaGeneral() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════════════════════\n");
        sb.append("SUGERENCIA GENERAL:\n");
        sb.append("- Revisar el SQL generado\n");
        sb.append("- Verificar los parámetros proporcionados\n");
        sb.append("- Consultar los logs para más detalles\n");
        sb.append("- Validar la estructura de la base de datos\n");
        sb.append("═══════════════════════════════════════════════════════════════════════════");
        return sb.toString();
    }
}