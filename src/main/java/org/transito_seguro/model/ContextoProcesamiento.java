package org.transito_seguro.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ContextoProcesamiento {

    private final ConcurrentLinkedQueue<Map<String, Object>> resultados = new ConcurrentLinkedQueue<>();

    // Método principal para agregar resultados de forma segura
    public void agregarResultado(Map<String, Object> resultado) {
        // Crear copia defensiva para evitar modificaciones externas
        Map<String, Object> copia = new HashMap<>(resultado);

        // Validación opcional de datos antes de agregar
        validarDatos(copia);

        resultados.add(copia);
    }

    // Método para agregar resultado con parámetros individuales
    public void agregarResultado(String fechaReporte, String municipio, int totalSinEmail, String provincia) {
        Map<String, Object> resultado = new HashMap<>();
        resultado.put("fecha_reporte", fechaReporte);
        resultado.put("municipio", municipio);
        resultado.put("total_sin_email", totalSinEmail);
        resultado.put("provincia", provincia);

        agregarResultado(resultado);
    }

    // Validación de datos para detectar posibles cruces
    private void validarDatos(Map<String, Object> resultado) {
        String municipio = (String) resultado.get("municipio");
        String provincia = (String) resultado.get("provincia");

        // Detectar posibles cruces como el que mencionaste
        if ((municipio != null && municipio.equalsIgnoreCase("Avellaneda") &&
                provincia != null && provincia.equalsIgnoreCase("LaPampa")) ||
                (municipio != null && municipio.equalsIgnoreCase("Santa Rosa") &&
                        provincia != null && provincia.equalsIgnoreCase("Avellaneda"))) {

            System.err.println("ADVERTENCIA: Posible cruce de datos detectado - " +
                    "municipio: " + municipio + ", provincia: " + provincia);
        }
    }

    // Obtener todos los resultados (seguro para lectura)
    public List<Map<String, Object>> getResultados() {
        return new ArrayList<>(resultados);
    }

    // Obtener resultados como JSON array
    public String getResultadosComoJson() {
        // Usar Jackson, Gson, o simplemente toString para ejemplo
        return resultados.toString();
    }

    // Método para limpiar resultados (si es necesario)
    public void limpiarResultados() {
        resultados.clear();
    }

    // Verificar si hay resultados
    public boolean tieneResultados() {
        return !resultados.isEmpty();
    }

    // Obtener cantidad de resultados
    public int cantidadResultados() {
        return resultados.size();
    }
}
