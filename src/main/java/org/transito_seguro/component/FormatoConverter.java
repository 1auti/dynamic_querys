package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class FormatoConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Convierte los datos al formato solicitado
     */
    public Object convertir(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            return generarRespuestaVacia(formato);
        }

        try {
            switch (formato.toLowerCase()) {
                case "json":
                    return convertirAJson(datos);
                case "csv":
                    return convertirACSV(datos);
                case "excel":
                    return convertirAExcel(datos);
                default:
                    throw new IllegalArgumentException("Formato no soportado: " + formato);
            }
        } catch (Exception e) {
            log.error("Error convirtiendo a formato {}: {}", formato, e.getMessage(), e);
            throw new RuntimeException("Error en conversión de formato", e);
        }
    }

    private Object convertirAJson(List<Map<String, Object>> datos) {
        try {
            Map<String, Object> mapa = new HashMap<>();
            mapa.put("datos",datos);
            return objectMapper.writeValueAsString(mapa);
        } catch (Exception e) {
            throw new RuntimeException("Error convirtiendo a JSON", e);
        }
    }

    private String convertirACSV(List<Map<String, Object>> datos) {
        try {
            StringWriter stringWriter = new StringWriter();

            // Usar configuración más básica para evitar conflictos
            CSVWriter csvWriter = new CSVWriter(stringWriter,
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // Headers
            if (!datos.isEmpty()) {
                String[] headers = datos.get(0).keySet().toArray(new String[0]);
                csvWriter.writeNext(headers);

                // Datos
                for (Map<String, Object> fila : datos) {
                    String[] valores = new String[headers.length];
                    for (int i = 0; i < headers.length; i++) {
                        Object valor = fila.get(headers[i]);
                        valores[i] = valor != null ? String.valueOf(valor) : "";
                    }
                    csvWriter.writeNext(valores);
                }
            }

            csvWriter.close();
            return stringWriter.toString();
        } catch (IOException e) {
            throw new RuntimeException("Error generando CSV", e);
        }
    }

    private byte[] convertirAExcel(List<Map<String, Object>> datos) {
        Workbook workbook = null;
        try {
            // Crear workbook usando constructor más simple
            workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Infracciones");

            if (!datos.isEmpty()) {
                // Headers
                Row headerRow = sheet.createRow(0);
                String[] headers = datos.get(0).keySet().toArray(new String[0]);

                // Crear estilo para headers
                CellStyle headerStyle = createHeaderStyle(workbook);

                for (int i = 0; i < headers.length; i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(headers[i]);
                    cell.setCellStyle(headerStyle);
                }

                // Datos
                for (int rowNum = 0; rowNum < datos.size(); rowNum++) {
                    Row row = sheet.createRow(rowNum + 1);
                    Map<String, Object> fila = datos.get(rowNum);

                    for (int colNum = 0; colNum < headers.length; colNum++) {
                        Cell cell = row.createCell(colNum);
                        Object valor = fila.get(headers[colNum]);
                        setCellValue(cell, valor);
                    }
                }

                // Auto-size columnas con límite
                autoSizeColumns(sheet, Math.min(headers.length, 20));
            }

            // Escribir a ByteArrayOutputStream
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            return outputStream.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException("Error generando Excel", e);
        } finally {
            if (workbook != null) {
                try {
                    workbook.close();
                } catch (IOException e) {
                    log.warn("Error cerrando workbook: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Crear estilo para headers de manera segura
     */
    private CellStyle createHeaderStyle(Workbook workbook) {
        try {
            CellStyle headerStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);
            return headerStyle;
        } catch (Exception e) {
            log.warn("No se pudo crear estilo de header: {}", e.getMessage());
            return workbook.createCellStyle(); // Estilo básico
        }
    }

    /**
     * Establecer valor de celda de manera segura
     */
    private void setCellValue(Cell cell, Object valor) {
        if (valor == null) {
            cell.setCellValue("");
            return;
        }

        try {
            // Intentar convertir a número si es posible
            if (valor instanceof Number) {
                cell.setCellValue(((Number) valor).doubleValue());
            } else if (valor instanceof Boolean) {
                cell.setCellValue((Boolean) valor);
            } else {
                cell.setCellValue(String.valueOf(valor));
            }
        } catch (Exception e) {
            // Fallback a string
            cell.setCellValue(String.valueOf(valor));
        }
    }

    /**
     * Auto-size columnas de manera segura
     */
    private void autoSizeColumns(Sheet sheet, int maxColumns) {
        try {
            for (int i = 0; i < maxColumns; i++) {
                sheet.autoSizeColumn(i);
            }
        } catch (Exception e) {
            log.warn("Error en auto-size de columnas: {}", e.getMessage());
        }
    }

    private Object generarRespuestaVacia(String formato) {
        switch (formato.toLowerCase()) {
            case "json":
                return "{\"total\": 0, \"datos\": []}";
            case "csv":
                return "";
            case "excel":
                return new byte[0];
            default:
                return null;
        }
    }
}