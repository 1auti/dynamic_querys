package org.transito_seguro.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public final class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private JsonUtils() {
        // Utility class - constructor privado
    }

    /**
     * Convierte una lista a JSON string
     */
    public static String toJson(List<String> lista) {
        if (lista == null || lista.isEmpty()) {
            return null;
        }

        try {
            return objectMapper.writeValueAsString(lista);
        } catch (JsonProcessingException e) {
            log.error("Error convirtiendo lista a JSON: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Convierte JSON string a lista de strings
     */
    public static List<String> fromJsonToList(String json) {
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyList();
        }

        try {
            TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
            return objectMapper.readValue(json, typeRef);
        } catch (Exception e) {
            log.error("Error convirtiendo JSON a lista: {} - JSON: {}", e.getMessage(), json);
            return Collections.emptyList();
        }
    }

    /**
     * Convierte cualquier objeto a JSON string
     */
    public static String toJson(Object objeto) {
        if (objeto == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsString(objeto);
        } catch (JsonProcessingException e) {
            log.error("Error convirtiendo objeto a JSON: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Convierte JSON string a objeto del tipo especificado
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }

        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("Error convirtiendo JSON a {}: {} - JSON: {}", clazz.getSimpleName(), e.getMessage(), json);
            return null;
        }
    }

    /**
     * Valida si una string es JSON válido
     */
    public static boolean isValidJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }

        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Convierte JSON a lista de objetos de tipo específico
     */
    public static <T> List<T> fromJsonToList(String json, Class<T> clazz) {
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyList();
        }

        try {
            TypeReference<List<T>> typeRef = new TypeReference<List<T>>() {};
            return objectMapper.readValue(json,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            log.error("Error convirtiendo JSON a lista de {}: {} - JSON: {}",
                    clazz.getSimpleName(), e.getMessage(), json);
            return Collections.emptyList();
        }
    }

    /**
     * Formatea JSON para lectura humana (pretty print)
     */
    public static String toPrettyJson(Object objeto) {
        if (objeto == null) {
            return null;
        }

        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objeto);
        } catch (JsonProcessingException e) {
            log.error("Error formateando JSON: {}", e.getMessage());
            return toJson(objeto); // Fallback a JSON sin formato
        }
    }

    /**
     * Compacta JSON eliminando espacios innecesarios
     */
    public static String toCompactJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return json;
        }

        try {
            Object obj = objectMapper.readValue(json, Object.class);
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Error compactando JSON: {}", e.getMessage());
            return json; // Retornar original si hay error
        }
    }
}