package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
    }

    private Object convertirAJson(List<Map<String, Object>> datos) {
        try {
            Map<String, Object> mapa = new HashMap<>();
            mapa.put("total", datos.size());
            mapa.put("datos", datos);
            return objectMapper.writeValueAsString(mapa);
        } catch (Exception e) {
            throw new RuntimeException("Error convirtiendo a JSON", e);
        }
    }

    private String convertirACSV(List<Map<String, Object>> datos) {
        try {
            StringWriter stringWriter = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(stringWriter);

            // Headers
            if (!datos.isEmpty()) {
                String[] headers = datos.get(0).keySet().toArray(new String[0]);
                csvWriter.writeNext(headers);

                // Datos
                for (Map<String, Object> fila : datos) {
                    String[] valores = headers.length > 0 ?
                            java.util.Arrays.stream(headers)
                                    .map(header -> String.valueOf(fila.get(header)))
                                    .toArray(String[]::new) : new String[0];
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
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Infracciones");

            if (!datos.isEmpty()) {
                // Headers
                Row headerRow = sheet.createRow(0);
                String[] headers = datos.get(0).keySet().toArray(new String[0]);

                for (int i = 0; i < headers.length; i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(headers[i]);
                    // Estilo para headers
                    CellStyle headerStyle = workbook.createCellStyle();
                    Font font = workbook.createFont();
                    font.setBold(true);
                    headerStyle.setFont(font);
                    cell.setCellStyle(headerStyle);
                }

                // Datos
                for (int rowNum = 0; rowNum < datos.size(); rowNum++) {
                    Row row = sheet.createRow(rowNum + 1);
                    Map<String, Object> fila = datos.get(rowNum);

                    for (int colNum = 0; colNum < headers.length; colNum++) {
                        Cell cell = row.createCell(colNum);
                        Object valor = fila.get(headers[colNum]);
                        if (valor != null) {
                            cell.setCellValue(String.valueOf(valor));
                        }
                    }
                }

                // Auto-size columnas
                for (int i = 0; i < headers.length; i++) {
                    sheet.autoSizeColumn(i);
                }
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            return outputStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Error generando Excel", e);
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
