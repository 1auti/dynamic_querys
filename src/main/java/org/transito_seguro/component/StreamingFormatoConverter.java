package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class StreamingFormatoConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Contexto para mantener estado durante procesamiento streaming
     */
    public static class StreamingContext {
        private final String formato;
        private final OutputStream outputStream;
        private final AtomicInteger totalRegistros = new AtomicInteger(0);
        private final AtomicLong bytesEscritos = new AtomicLong(0);

        // Para CSV
        private CSVWriter csvWriter;
        private boolean csvHeadersEscritos = false;

        // Para Excel
        private XSSFWorkbook workbook;
        private Sheet sheet;
        private int currentRowIndex = 0;

        // Para JSON
        private boolean jsonPrimerRegistro = true;
        private PrintWriter jsonWriter;

        public StreamingContext(String formato, OutputStream outputStream) {
            this.formato = formato.toLowerCase();
            this.outputStream = outputStream;
        }

        // Getters
        public String getFormato() { return formato; }
        public OutputStream getOutputStream() { return outputStream; }
        public int getTotalRegistros() { return totalRegistros.get(); }
        public long getBytesEscritos() { return bytesEscritos.get(); }
    }

    /**
     * Inicializa el contexto de streaming según el formato
     */
    public StreamingContext inicializarStreaming(String formato, OutputStream outputStream) throws IOException {
        StreamingContext context = new StreamingContext(formato, outputStream);

        switch (formato.toLowerCase()) {
            case "csv":
                inicializarCSVStreaming(context);
                break;
            case "json":
                inicializarJSONStreaming(context);
                break;
            case "excel":
                inicializarExcelStreaming(context);
                break;
            default:
                throw new IllegalArgumentException("Formato no soportado para streaming: " + formato);
        }

        log.debug("Streaming inicializado para formato: {}", formato);
        return context;
    }

    /**
     * Procesa un lote de datos en streaming
     */
    public void procesarLoteStreaming(StreamingContext context, List<Map<String, Object>> lote) throws IOException {
        if (lote == null || lote.isEmpty()) {
            return;
        }

        switch (context.getFormato()) {
            case "csv":
                procesarLoteCSV(context, lote);
                break;
            case "json":
                procesarLoteJSON(context, lote);
                break;
            case "excel":
                procesarLoteExcel(context, lote);
                break;
        }

        context.totalRegistros.addAndGet(lote.size());
        log.debug("Lote procesado: {} registros, total acumulado: {}",
                lote.size(), context.getTotalRegistros());
    }

    /**
     * Finaliza el streaming y cierra recursos
     */
    public void finalizarStreaming(StreamingContext context) throws IOException {
        switch (context.getFormato()) {
            case "csv":
                finalizarCSVStreaming(context);
                break;
            case "json":
                finalizarJSONStreaming(context);
                break;
            case "excel":
                finalizarExcelStreaming(context);
                break;
        }

        log.info("Streaming finalizado: {} registros, {} bytes escritos",
                context.getTotalRegistros(), context.getBytesEscritos());
    }

    // =========================== CSV STREAMING ===========================

    private void inicializarCSVStreaming(StreamingContext context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(context.getOutputStream(), "UTF-8");
        context.csvWriter = new CSVWriter(writer);
    }

    private void procesarLoteCSV(StreamingContext context, List<Map<String, Object>> lote) throws IOException {
        if (!context.csvHeadersEscritos && !lote.isEmpty()) {
            // Escribir headers solo una vez
            Set<String> headers = lote.get(0).keySet();
            context.csvWriter.writeNext(headers.toArray(new String[0]));
            context.csvHeadersEscritos = true;
        }

        // Escribir datos
        for (Map<String, Object> registro : lote) {
            String[] valores = registro.values().stream()
                    .map(v -> v != null ? String.valueOf(v) : "")
                    .toArray(String[]::new);
            context.csvWriter.writeNext(valores);
        }

        context.csvWriter.flush();
    }

    private void finalizarCSVStreaming(StreamingContext context) throws IOException {
        if (context.csvWriter != null) {
            context.csvWriter.close();
        }
    }

    // =========================== JSON STREAMING ===========================

    private void inicializarJSONStreaming(StreamingContext context) throws IOException {
        context.jsonWriter = new PrintWriter(new OutputStreamWriter(context.getOutputStream(), "UTF-8"));
        context.jsonWriter.println("{");
        context.jsonWriter.println("  \"total\": 0,");
        context.jsonWriter.println("  \"datos\": [");
    }

    private void procesarLoteJSON(StreamingContext context, List<Map<String, Object>> lote) throws IOException {
        for (Map<String, Object> registro : lote) {
            if (!context.jsonPrimerRegistro) {
                context.jsonWriter.println(",");
            }

            String registroJson = objectMapper.writeValueAsString(registro);
            context.jsonWriter.print("    " + registroJson);

            context.jsonPrimerRegistro = false;
        }
        context.jsonWriter.flush();
    }

    private void finalizarJSONStreaming(StreamingContext context) throws IOException {
        if (context.jsonWriter != null) {
            context.jsonWriter.println();
            context.jsonWriter.println("  ],");
            context.jsonWriter.println("  \"total\": " + context.getTotalRegistros());
            context.jsonWriter.println("}");
            context.jsonWriter.close();
        }
    }

    // =========================== EXCEL STREAMING ===========================

    private void inicializarExcelStreaming(StreamingContext context) {
        context.workbook = new XSSFWorkbook();
        context.sheet = context.workbook.createSheet("Infracciones");
    }

    private void procesarLoteExcel(StreamingContext context, List<Map<String, Object>> lote) {
        if (lote.isEmpty()) return;

        // Escribir headers solo una vez
        if (context.currentRowIndex == 0) {
            Row headerRow = context.sheet.createRow(0);
            String[] headers = lote.get(0).keySet().toArray(new String[0]);

            CellStyle headerStyle = context.workbook.createCellStyle();
            Font font = context.workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);

            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }
            context.currentRowIndex = 1;
        }

        // Escribir datos
        String[] headers = lote.get(0).keySet().toArray(new String[0]);
        for (Map<String, Object> registro : lote) {
            Row row = context.sheet.createRow(context.currentRowIndex++);

            for (int i = 0; i < headers.length; i++) {
                Cell cell = row.createCell(i);
                Object valor = registro.get(headers[i]);
                if (valor != null) {
                    cell.setCellValue(String.valueOf(valor));
                }
            }
        }

        // Auto-size columnas periódicamente (cada 1000 registros)
        if (context.currentRowIndex % 1000 == 0) {
            for (int i = 0; i < headers.length && i < 20; i++) { // Limitar a 20 columnas
                context.sheet.autoSizeColumn(i);
            }
        }
    }

    private void finalizarExcelStreaming(StreamingContext context) throws IOException {
        if (context.workbook != null) {
            // Auto-size final de columnas
            if (context.sheet.getLastRowNum() > 0) {
                Row firstRow = context.sheet.getRow(0);
                if (firstRow != null) {
                    int numCols = Math.min(firstRow.getLastCellNum(), 20);
                    for (int i = 0; i < numCols; i++) {
                        context.sheet.autoSizeColumn(i);
                    }
                }
            }

            context.workbook.write(context.getOutputStream());
            context.workbook.close();
        }
    }

    /**
     * Método de utilidad para crear un OutputStream en memoria con límite
     */
    public static class LimitedByteArrayOutputStream extends ByteArrayOutputStream {
        private final int maxSize;

        public LimitedByteArrayOutputStream(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public void write(int b) {
            if (count >= maxSize) {
                throw new RuntimeException("Límite de memoria excedido: " + maxSize + " bytes");
            }
            super.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            if (count + len > maxSize) {
                throw new RuntimeException("Límite de memoria excedido: " + maxSize + " bytes");
            }
            super.write(b, off, len);
        }
    }
}