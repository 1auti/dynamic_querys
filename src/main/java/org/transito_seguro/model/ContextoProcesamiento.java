package org.transito_seguro.model;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Clase que representa el contexto de procesamiento de resultados.
 * Permite agregar, validar, consultar y limpiar resultados de manera segura
 * en entornos concurrentes.
 */
public class ContextoProcesamiento {

    private static final Logger LOGGER = Logger.getLogger(ContextoProcesamiento.class.getName());

    /** Cola concurrente para almacenar los resultados finales. */
    private final ConcurrentLinkedQueue<Map<String, Object>> resultados = new ConcurrentLinkedQueue<>();

    /** Cola para resultados intermedios o parciales. */
    private final ConcurrentLinkedQueue<Map<String, Object>> resultadosParciales = new ConcurrentLinkedQueue<>();

    /** Procesador que se aplica sobre lotes de resultados. */
    private final Consumer<List<Map<String, Object>>> procesador;

    /** Archivo de salida opcional para escritura directa. */
    private final BufferedWriter archivo;

    /** Serializador JSON. */
    private final ObjectMapper mapper = new ObjectMapper();

    // =====================================================
    // CONSTRUCTOR
    // =====================================================

    public ContextoProcesamiento(Consumer<List<Map<String, Object>>> procesador, BufferedWriter archivo) {
        this.procesador = procesador;
        this.archivo = archivo;
    }

    // =====================================================
    // MÉTODOS PÚBLICOS PRINCIPALES
    // =====================================================

    /**
     * Agrega múltiples resultados al contexto de manera segura.
     *
     * @param resultados Lista de resultados a agregar.
     */
    public void agregarResultados(List<Map<String, Object>> resultados) {
        if (resultados != null && !resultados.isEmpty()) {
            resultadosParciales.addAll(resultados);
        }
    }

    /**
     * Procesa todos los resultados almacenados en la cola parcial.
     * Dependiendo de la configuración, los envía al procesador o los escribe en archivo.
     */
    public synchronized void procesarTodosResultados() {
        if (resultadosParciales.isEmpty()) return;

        List<Map<String, Object>> todosResultados = new ArrayList<>(resultadosParciales);
        resultadosParciales.clear();

        if (procesador != null) {
            procesador.accept(todosResultados);
        } else if (archivo != null) {
            escribirArchivo(todosResultados);
        }
    }

    /**
     * Agrega un resultado individual al contexto de manera segura.
     *
     * @param resultado Mapa con los datos del resultado.
     */
    public void agregarResultado(Map<String, Object> resultado) {
        if (resultado == null || resultado.isEmpty()) return;

        Map<String, Object> copia = new HashMap<>(resultado);
        validarDatos(copia);
        resultados.add(copia);
    }

    /**
     * Agrega un resultado utilizando parámetros individuales.
     */
    public void agregarResultado(String fechaReporte, String municipio, int totalSinEmail, String provincia) {
        Map<String, Object> resultado = new HashMap<>();
        resultado.put("fecha_reporte", fechaReporte);
        resultado.put("municipio", municipio);
        resultado.put("total_sin_email", totalSinEmail);
        resultado.put("provincia", provincia);
        agregarResultado(resultado);
    }

    /**
     * Escribe una lista de resultados en el archivo asociado (si fue configurado).
     */
    public void escribirArchivo(List<Map<String, Object>> resultados) {
        try {
            for (Map<String, Object> resultado : resultados) {
                archivo.write(mapper.writeValueAsString(resultado));
                archivo.newLine();
            }
            archivo.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error escribiendo archivo", e);
        }
    }

    // =====================================================
    // MÉTODOS DE CONSULTA
    // =====================================================

    public List<Map<String, Object>> getResultados() {
        return new ArrayList<>(resultados);
    }

    public String getResultadosComoJson() {
        return resultados.toString(); // o mapper.writeValueAsString(resultados)
    }

    public void limpiarResultados() {
        resultados.clear();
    }

    public boolean tieneResultados() {
        return !resultados.isEmpty();
    }

    public int cantidadResultados() {
        return resultados.size();
    }

    // =====================================================
    // MÉTODOS PRIVADOS AUXILIARES
    // =====================================================

    /**
     * Valida los datos del resultado y detecta posibles cruces de provincia/municipio.
     */
    private void validarDatos(Map<String, Object> resultado) {
        String municipio = (String) resultado.get("municipio");
        String provincia = (String) resultado.get("provincia");

        if ((municipio != null && municipio.equalsIgnoreCase("Avellaneda") &&
                provincia != null && provincia.equalsIgnoreCase("LaPampa")) ||
            (municipio != null && municipio.equalsIgnoreCase("Santa Rosa") &&
                provincia != null && provincia.equalsIgnoreCase("Avellaneda"))) {

            LOGGER.log(Level.WARNING,
                    "Posible cruce de datos detectado - municipio: {0}, provincia: {1}",
                    new Object[]{municipio, provincia});
        }
    }
}
