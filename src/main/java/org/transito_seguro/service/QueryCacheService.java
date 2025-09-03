package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class QueryCacheService {

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutos
    private static final int MAX_CACHE_SIZE = 1000;

    /**
     * Obtiene resultado desde cache si existe y no está expirado
     */
    public List<Map<String, Object>> getCachedResult(String query, ParametrosFiltrosDTO filtros) {
        String cacheKey = generateCacheKey(query, filtros);
        CacheEntry entry = cache.get(cacheKey);

        if (entry != null && !entry.isExpired()) {
            log.debug("Cache hit para key: {}", cacheKey);
            return entry.getData();
        }

        if (entry != null) {
            cache.remove(cacheKey); // Limpiar entrada expirada
            log.debug("Cache miss - entrada expirada para key: {}", cacheKey);
        }

        return null;
    }

    /**
     * Almacena resultado en cache
     */
    public void cacheResult(String query, ParametrosFiltrosDTO filtros, List<Map<String, Object>> result) {
        // No cachear resultados muy grandes
        if (result.size() > 10000) {
            log.debug("No cacheando resultado grande: {} registros", result.size());
            return;
        }

        String cacheKey = generateCacheKey(query, filtros);

        // Limpiar cache si está muy lleno
        if (cache.size() >= MAX_CACHE_SIZE) {
            cleanExpiredEntries();
        }

        cache.put(cacheKey, new CacheEntry(result));
        log.debug("Resultado cacheado para key: {} ({} registros)", cacheKey, result.size());
    }

    /**
     * Verifica si una consulta es cacheable
     */
    public boolean isCacheable(ParametrosFiltrosDTO filtros) {
        // No cachear consultas con fechas muy recientes (pueden cambiar)
        if (filtros.getFechaEspecifica() != null) {
            long daysDiff = (System.currentTimeMillis() - filtros.getFechaEspecifica().getTime()) / (24 * 60 * 60 * 1000);
            return daysDiff > 1; // Solo cachear consultas de hace más de 1 día
        }

        // No cachear consultas muy específicas (pocos resultados, no vale la pena)
        if (filtros.getDominios() != null && filtros.getDominios().size() < 10) {
            return false;
        }

        return true;
    }

    /**
     * Genera clave de cache basada en query y filtros
     */
    private String generateCacheKey(String query, ParametrosFiltrosDTO filtros) {
        try {
            String keyData = query + "|" + filtros.toString();
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(keyData.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString().substring(0, 16); // Usar primeros 16 caracteres

        } catch (Exception e) {
            // Fallback a hashCode si SHA-256 falla
            return String.valueOf((query + filtros.toString()).hashCode());
        }
    }

    /**
     * Limpia entradas expiradas del cache
     */
    private void cleanExpiredEntries() {
        int initialSize = cache.size();
        cache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        int cleaned = initialSize - cache.size();

        if (cleaned > 0) {
            log.debug("Limpiadas {} entradas expiradas del cache", cleaned);
        }
    }

    /**
     * Obtiene estadísticas del cache
     */
    public Map<String, Object> getCacheStats() {
        cleanExpiredEntries();

        Map<String, Object> result = new HashMap<>();
        result.put("totalEntries", cache.size());
        result.put("maxSize", MAX_CACHE_SIZE);
        result.put("ttlMinutes", CACHE_TTL_MS / (60 * 1000));
        result.put("memoryUsageApprox", cache.size() * 1000 + " bytes (estimado)");
        return result;
    }

    // Clase interna para entradas de cache
    private static class CacheEntry {
        private final List<Map<String, Object>> data;
        private final long timestamp;

        public CacheEntry(List<Map<String, Object>> data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }

        public List<Map<String, Object>> getData() {
            return data;
        }
    }
}