package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class RateLimitingFilter extends OncePerRequestFilter {

    private final ConcurrentHashMap<String, RequestWindow> requestWindows = new ConcurrentHashMap<>();

    // Configuración de rate limiting
    private static final int REQUESTS_PER_MINUTE = 30;
    private static final int TASKS_PER_HOUR_PER_IP = 10;
    private static final long WINDOW_SIZE_MS = 60 * 1000; // 1 minuto

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String clientIp = getClientIpAddress(request);
        String requestUri = request.getRequestURI();

        // Solo aplicar rate limiting a endpoints de API
        if (!requestUri.startsWith("/api/")) {
            filterChain.doFilter(request, response);
            return;
        }

        // Rate limiting más estricto para endpoints de tareas
        boolean isTaskEndpoint = requestUri.startsWith("/api/tareas/") &&
                request.getMethod().equals("POST");

        if (isTaskEndpoint) {
            if (!checkTaskRateLimit(clientIp)) {
                response.setStatus(429);
                response.getWriter().write("{\"error\":\"Límite de tareas por hora excedido\",\"limite\":10}");
                return;
            }
        }

        // Rate limiting general
        if (!checkGeneralRateLimit(clientIp)) {
            response.setStatus(429);
            response.setHeader("Retry-After", "60");
            response.getWriter().write("{\"error\":\"Límite de requests por minuto excedido\",\"limite\":30}");
            return;
        }

        // Agregar headers informativos
        RequestWindow window = requestWindows.get(clientIp);
        if (window != null) {
            response.setHeader("X-RateLimit-Limit", String.valueOf(REQUESTS_PER_MINUTE));
            response.setHeader("X-RateLimit-Remaining",
                    String.valueOf(Math.max(0, REQUESTS_PER_MINUTE - window.getRequestCount())));
        }

        filterChain.doFilter(request, response);
    }

    private boolean checkGeneralRateLimit(String clientIp) {
        long now = System.currentTimeMillis();

        RequestWindow window = requestWindows.computeIfAbsent(clientIp, k -> new RequestWindow());

        // Limpiar requests antiguos
        window.cleanOldRequests(now - WINDOW_SIZE_MS);

        if (window.getRequestCount() >= REQUESTS_PER_MINUTE) {
            log.warn("Rate limit excedido para IP: {} ({} requests)", clientIp, window.getRequestCount());
            return false;
        }

        window.addRequest(now);
        return true;
    }

    private boolean checkTaskRateLimit(String clientIp) {
        // Implementar lógica específica para tareas (por hora)
        // Para simplicidad, usamos la misma ventana pero con límite diferente
        RequestWindow window = requestWindows.computeIfAbsent(clientIp + "_tasks", k -> new RequestWindow());

        long oneHourAgo = System.currentTimeMillis() - (60 * 60 * 1000);
        window.cleanOldRequests(oneHourAgo);

        if (window.getRequestCount() >= TASKS_PER_HOUR_PER_IP) {
            log.warn("Task rate limit excedido para IP: {} ({} tareas en la última hora)",
                    clientIp, window.getRequestCount());
            return false;
        }

        window.addRequest(System.currentTimeMillis());
        return true;
    }

    private String getClientIpAddress(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isEmpty()) {
            return xForwardedFor.split(",")[0].trim();
        }

        String xRealIp = request.getHeader("X-Real-IP");
        if (xRealIp != null && !xRealIp.isEmpty()) {
            return xRealIp;
        }

        return request.getRemoteAddr();
    }

    // Clase interna para manejar ventanas de requests
    private static class RequestWindow {
        private final ConcurrentLinkedQueue<Long> requests = new ConcurrentLinkedQueue<>();

        public void addRequest(long timestamp) {
            requests.offer(timestamp);
        }

        public void cleanOldRequests(long cutoffTime) {
            requests.removeIf(timestamp -> timestamp < cutoffTime);
        }

        public int getRequestCount() {
            return requests.size();
        }
    }
}
