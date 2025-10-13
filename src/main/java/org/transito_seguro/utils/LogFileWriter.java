package org.transito_seguro.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Componente para generar archivos de logs detallados de procesos.
 * Permite tener un registro persistente y legible de las ejecuciones.
 *
 * Buenas prácticas implementadas:
 * - Thread-safe usando synchronized
 * - Manejo robusto de excepciones
 * - Formato legible con timestamps
 * - Separación por categorías de log
 * - Auto-creación de directorios
 */
@Component
@Slf4j
public class LogFileWriter {

    // Directorio base donde se guardarán los logs
    private static final String LOG_DIR = "logs/procesos";

    // Formato de fecha para nombres de archivo y timestamps
    private static final DateTimeFormatter FILE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // Writer actual del archivo de log
    private BufferedWriter currentWriter;

    // Ruta del archivo actual
    private Path currentLogFile;

    // Tiempo de inicio del proceso
    private LocalDateTime startTime;

    /**
     * Inicializa un nuevo archivo de log para un proceso.
     *
     * @param nombreProceso Nombre descriptivo del proceso
     * @param nombreQuery Código o nombre de la query ejecutada
     * @return Path del archivo creado
     * @throws IOException Si hay error al crear el archivo
     */
    public synchronized Path iniciarLog(String nombreProceso, String nombreQuery) throws IOException {
        // Cerrar writer anterior si existe
        cerrarLogActual();

        // Crear directorio si no existe
        Path dirPath = Paths.get(LOG_DIR);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            log.info("📁 Directorio de logs creado: {}", dirPath.toAbsolutePath());
        }

        // Generar nombre de archivo con timestamp
        startTime = LocalDateTime.now();
        String timestamp = startTime.format(FILE_FORMATTER);
        String nombreArchivo = String.format("%s_%s_%s.log",
                nombreProceso.replaceAll("\\s+", "_"),
                nombreQuery.replaceAll("\\s+", "_"),
                timestamp
        );

        // Crear archivo
        currentLogFile = dirPath.resolve(nombreArchivo);
        currentWriter = Files.newBufferedWriter(
                currentLogFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Escribir encabezado
        escribirEncabezado(nombreProceso, nombreQuery);

        log.info("📝 Archivo de log creado: {}", currentLogFile.toAbsolutePath());

        return currentLogFile;
    }

    /**
     * Escribe el encabezado inicial del archivo de log.
     */
    private void escribirEncabezado(String nombreProceso, String nombreQuery) throws IOException {
        currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
        currentWriter.write("  LOG DE PROCESO - TRANSITO SEGURO\n");
        currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
        currentWriter.write(String.format("Proceso:    %s\n", nombreProceso));
        currentWriter.write(String.format("Query:      %s\n", nombreQuery));
        currentWriter.write(String.format("Inicio:     %s\n", startTime.format(TIMESTAMP_FORMATTER)));
        currentWriter.write(String.format("Sistema:    Java %s\n", System.getProperty("java.version")));
        currentWriter.write("═══════════════════════════════════════════════════════════════════════\n\n");
        currentWriter.flush();
    }

    /**
     * Escribe una línea de log con nivel INFO.
     *
     * @param mensaje Mensaje a registrar
     */
    public synchronized void info(String mensaje) {
        escribirLinea("INFO", mensaje);
    }

    /**
     * Escribe una línea de log con nivel INFO y argumentos formateados.
     *
     * @param formato Formato del mensaje (estilo String.format)
     * @param args Argumentos del formato
     */
    public synchronized void info(String formato, Object... args) {
        escribirLinea("INFO", String.format(formato, args));
    }

    /**
     * Escribe una línea de log con nivel WARN.
     *
     * @param mensaje Mensaje de advertencia
     */
    public synchronized void warn(String mensaje) {
        escribirLinea("WARN", mensaje);
    }

    /**
     * Escribe una línea de log con nivel ERROR.
     *
     * @param mensaje Mensaje de error
     * @param throwable Excepción asociada (puede ser null)
     */
    public synchronized void error(String mensaje, Throwable throwable) {
        escribirLinea("ERROR", mensaje);
        if (throwable != null) {
            escribirStackTrace(throwable);
        }
    }

    /**
     * Escribe una línea de log con nivel DEBUG.
     *
     * @param mensaje Mensaje de debug
     */
    public synchronized void debug(String mensaje) {
        escribirLinea("DEBUG", mensaje);
    }

    /**
     * Escribe un separador visual en el log.
     */
    public synchronized void separador() {
        escribirLineaDirecta("───────────────────────────────────────────────────────────────────────\n");
    }

    /**
     * Escribe un separador con título.
     *
     * @param titulo Título de la sección
     */
    public synchronized void seccion(String titulo) {
        try {
            if (currentWriter != null) {
                currentWriter.write("\n");
                currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
                currentWriter.write(String.format("  %s\n", titulo.toUpperCase()));
                currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
                currentWriter.flush();
            }
        } catch (IOException e) {
            log.error("Error escribiendo sección en log", e);
        }
    }

    /**
     * Escribe estadísticas del proceso en formato tabla.
     *
     * @param estadisticas Mapa con pares clave-valor de estadísticas
     */
    public synchronized void estadisticas(java.util.Map<String, Object> estadisticas) {
        try {
            if (currentWriter != null) {
                currentWriter.write("\n📊 ESTADÍSTICAS DEL PROCESO:\n");
                currentWriter.write("─────────────────────────────────────────────\n");

                estadisticas.forEach((clave, valor) -> {
                    try {
                        currentWriter.write(String.format("  %-30s : %s\n", clave, valor));
                    } catch (IOException e) {
                        log.error("Error escribiendo estadística", e);
                    }
                });

                currentWriter.write("─────────────────────────────────────────────\n\n");
                currentWriter.flush();
            }
        } catch (IOException e) {
            log.error("Error escribiendo estadísticas en log", e);
        }
    }

    /**
     * Finaliza el log escribiendo resumen y cerrando el archivo.
     *
     * @param totalRegistros Total de registros procesados
     * @param exitoso Indica si el proceso finalizó exitosamente
     */
    public synchronized void finalizarLog(int totalRegistros, boolean exitoso) {
        try {
            if (currentWriter != null) {
                LocalDateTime endTime = LocalDateTime.now();
                long duracionMs = java.time.Duration.between(startTime, endTime).toMillis();

                currentWriter.write("\n");
                currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
                currentWriter.write("  RESUMEN FINAL\n");
                currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");
                currentWriter.write(String.format("Estado:              %s\n",
                        exitoso ? "✅ COMPLETADO" : "❌ ERROR"));
                currentWriter.write(String.format("Registros totales:   %,d\n", totalRegistros));
                currentWriter.write(String.format("Duración:            %.2f segundos\n", duracionMs / 1000.0));
                currentWriter.write(String.format("Velocidad:           %,d reg/s\n",
                        duracionMs > 0 ? (totalRegistros * 1000L) / duracionMs : 0));
                currentWriter.write(String.format("Finalización:        %s\n",
                        endTime.format(TIMESTAMP_FORMATTER)));
                currentWriter.write("═══════════════════════════════════════════════════════════════════════\n");

                log.info("✅ Log finalizado: {}", currentLogFile.toAbsolutePath());
            }
        } catch (IOException e) {
            log.error("Error finalizando log", e);
        } finally {
            cerrarLogActual();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // MÉTODOS PRIVADOS
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Escribe una línea de log con nivel y timestamp.
     */
    private void escribirLinea(String nivel, String mensaje) {
        try {
            if (currentWriter != null) {
                String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
                String linea = String.format("[%s] [%s] %s\n", timestamp, nivel, mensaje);
                currentWriter.write(linea);
                currentWriter.flush();
            }
        } catch (IOException e) {
            log.error("Error escribiendo en log", e);
        }
    }

    /**
     * Escribe una línea directa sin formato.
     */
    private void escribirLineaDirecta(String linea) {
        try {
            if (currentWriter != null) {
                currentWriter.write(linea);
                currentWriter.flush();
            }
        } catch (IOException e) {
            log.error("Error escribiendo en log", e);
        }
    }

    /**
     * Escribe el stack trace de una excepción.
     */
    private void escribirStackTrace(Throwable throwable) {
        try {
            if (currentWriter != null) {
                currentWriter.write("  Stack Trace:\n");
                currentWriter.write("  " + throwable.toString() + "\n");
                for (StackTraceElement element : throwable.getStackTrace()) {
                    currentWriter.write("    at " + element.toString() + "\n");
                }
                currentWriter.write("\n");
                currentWriter.flush();
            }
        } catch (IOException e) {
            log.error("Error escribiendo stack trace", e);
        }
    }

    /**
     * Cierra el writer del log actual si existe.
     */
    private void cerrarLogActual() {
        if (currentWriter != null) {
            try {
                currentWriter.close();
            } catch (IOException e) {
                log.error("Error cerrando writer de log", e);
            } finally {
                currentWriter = null;
                currentLogFile = null;
            }
        }
    }
}