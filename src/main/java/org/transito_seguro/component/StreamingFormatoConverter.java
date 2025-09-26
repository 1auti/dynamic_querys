package org.transito_seguro.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
    @Getter
    @AllArgsConstructor
    public static class StreamingContext {
        private final String formato;
        private final OutputStream outputStream;
        private final AtomicInteger totalRegistros = new AtomicInteger(0);
        private final AtomicLong bytesEscritos = new AtomicLong(0);
        private CoutingOutputStream coutingOutputStream;

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

        private StreamingContext(String formato, OutputStream outputStream) {
            this.formato = formato;
            this.outputStream = outputStream;
        }

    }

    /**
     * OutputStream wrapper que cuenta bytes escritos
     * */
    private static class CoutingOutputStream extends OutputStream{
        private final OutputStream wrapped;
        private final AtomicLong bytesWritten;

        private CoutingOutputStream(OutputStream wrapped, AtomicLong bytesWritten) {
            this.wrapped = wrapped;
            this.bytesWritten = bytesWritten;
        }

        @Override
        public void write(int b) throws IOException {
            wrapped.write(b);
            bytesWritten.incrementAndGet();
        }

        @Override
        public void write(byte[] b) throws IOException {
            wrapped.write(b);
            bytesWritten.addAndGet(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            wrapped.write(b, off, len);
            bytesWritten.addAndGet(len);
        }

        @Override
        public void flush() throws IOException {
            wrapped.flush();
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }

    /**
     * Inicializa el contexto de streaming según el formato
     */
    public StreamingContext inicializarStreaming(String formato, OutputStream outputStream) throws IOException {
        if(outputStream == null) throw new IllegalArgumentException("El outputStream esta vacio -> NULL");
        StreamingContext context = new StreamingContext(formato, outputStream);

        CoutingOutputStream coutingOutputStream = new CoutingOutputStream(outputStream,context.bytesEscritos);
        context.coutingOutputStream = coutingOutputStream;

        switch (formato.toLowerCase()) {
            case "csv":
                inicializarCSVStreaming(context,coutingOutputStream);
                break;
            case "json":
                inicializarJSONStreaming(context,coutingOutputStream);
                break;
            case "excel":
                inicializarExcelStreaming(context,coutingOutputStream);
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
            log.warn("Lote vacío recibido para formato: {}", context.getFormato());
            return;
        }

        log.debug("Iniciando procesamiento de lote: {} registros, formato: {}",
                lote.size(), context.getFormato());

        try {
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
                default:
                    throw new IllegalStateException("Formato no reconocido: " + context.getFormato());
            }

            context.totalRegistros.addAndGet(lote.size());

            // Forzar flush del OutputStream principal
            context.getOutputStream().flush();

            log.debug("Lote procesado exitosamente: {} registros, total acumulado: {}",
                    lote.size(), context.getTotalRegistros());

        } catch (IOException e) {
            log.error("Error procesando lote de {} registros", lote.size(), e);
            throw e;
        }
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

    private void inicializarCSVStreaming(StreamingContext context, CoutingOutputStream countingStream) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(countingStream, "UTF-8");
        context.csvWriter = new CSVWriter(writer);
    }

    private void procesarLoteCSV(StreamingContext context, List<Map<String, Object>> lote) throws IOException {
        if (!context.csvHeadersEscritos && !lote.isEmpty()) {
            Set<String> headers = lote.get(0).keySet();
            context.csvWriter.writeNext(headers.toArray(new String[0]));
            context.csvHeadersEscritos = true;
        }

        // Procesar en sub-chunks para liberar memoria más frecuentemente
        int subChunkSize = Math.min(50, lote.size()); // Sub-chunks de 50

        for (int i = 0; i < lote.size(); i += subChunkSize) {
            int endIndex = Math.min(i + subChunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                Map<String, Object> registro = lote.get(j);
                String[] valores = registro.values().stream()
                        .map(v -> v != null ? String.valueOf(v) : "")
                        .toArray(String[]::new);
                context.csvWriter.writeNext(valores);
            }

            // Flush cada sub-chunk
            context.csvWriter.flush();
        }
    }

    private void finalizarCSVStreaming(StreamingContext context) throws IOException {
        if (context.csvWriter != null) {
            context.csvWriter.close();
        }
    }

    // =========================== JSON STREAMING ===========================

    private void inicializarJSONStreaming(StreamingContext context, CoutingOutputStream countingStream) throws IOException {
        context.jsonWriter = new PrintWriter(new OutputStreamWriter(countingStream, "UTF-8"));
        context.jsonWriter.println("{");
        context.jsonWriter.println("  \"datos\": [");
        context.jsonWriter.flush();
    }

    private void procesarLoteJSON(StreamingContext context, List<Map<String, Object>> lote) throws IOException {
        log.debug("Procesando lote JSON - {} registros, bytes antes: {}",
                lote.size(), context.getBytesEscritos());
        int subChunkSize = Math.min(25, lote.size()); // Sub-chunks más pequeños para JSON

        for (int i = 0; i < lote.size(); i += subChunkSize) {
            int endIndex = Math.min(i + subChunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                Map<String, Object> registro = lote.get(j);

                if (!context.jsonPrimerRegistro) {
                    context.jsonWriter.println(",");
                }

                String registroJson = objectMapper.writeValueAsString(registro);
                context.jsonWriter.print("    " + registroJson);
                context.jsonPrimerRegistro = false;
            }

            log.debug("Lote JSON completado - bytes después: {}", context.getBytesEscritos());

            // Flush cada sub-chunk
            context.jsonWriter.flush();
        }
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

    private void inicializarExcelStreaming(StreamingContext context, CoutingOutputStream countingStream) {
        context.workbook = new XSSFWorkbook();
        context.sheet = context.workbook.createSheet("Infracciones");
        context.coutingOutputStream = countingStream;
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

        // Procesar en sub-chunks pequeños
        String[] headers = lote.get(0).keySet().toArray(new String[0]);
        int subChunkSize = Math.min(100, lote.size()); // Sub-chunks de 100 para Excel

        for (int i = 0; i < lote.size(); i += subChunkSize) {
            int endIndex = Math.min(i + subChunkSize, lote.size());

            for (int j = i; j < endIndex; j++) {
                Map<String, Object> registro = lote.get(j);
                Row row = context.sheet.createRow(context.currentRowIndex++);

                for (int k = 0; k < headers.length; k++) {
                    Cell cell = row.createCell(k);
                    Object valor = registro.get(headers[k]);
                    if (valor != null) {
                        cell.setCellValue(String.valueOf(valor));
                    }
                }
            }

            // NUEVO: Limpiar referencias periódicamente para Excel
            if ((i / subChunkSize) % 5 == 0) {
                // Excel mantiene todo en memoria, solo podemos sugerir GC
                System.gc();
            }
        }

        // Auto-size cada 500 registros (menos frecuente para performance)
        if (context.currentRowIndex % 500 == 0) {
            for (int i = 0; i < headers.length && i < 15; i++) {
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

            // Escribir al OutputStream counting
            context.workbook.write(context.coutingOutputStream);
            context.coutingOutputStream.flush(); // Flush explícito
            context.workbook.close();

            log.debug("Excel finalizado - Total registros: {}, Bytes escritos: {}",
                    context.getTotalRegistros(), context.getBytesEscritos());
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