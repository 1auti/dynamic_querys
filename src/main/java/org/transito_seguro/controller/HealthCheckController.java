package org.transito_seguro.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.transito_seguro.service.TaskService;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/salud")
public class HealthCheckController {

    @Autowired
    private TaskService taskService;

    /**
     * Health check específico para el sistema asíncrono
     */
    @GetMapping("/tareas")
    public ResponseEntity<Map<String, Object>> saludTareas() {
        Map<String, Object> salud = new HashMap<>();

        try {
            Map<String, Object> stats = taskService.obtenerEstadisticas();

            salud.put("estado", "SALUDABLE");
            salud.put("estadisticas", stats);
            salud.put("timestamp", System.currentTimeMillis());

            return ResponseEntity.ok(salud);

        } catch (Exception e) {
            salud.put("estado", "ERROR");
            salud.put("error", e.getMessage());
            salud.put("timestamp", System.currentTimeMillis());

            return ResponseEntity.status(500).body(salud);
        }
    }
}