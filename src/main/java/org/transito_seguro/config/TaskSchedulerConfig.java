package org.transito_seguro.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.transito_seguro.service.TaskService;

@Configuration
@EnableScheduling
public class TaskSchedulerConfig {

    @Autowired
    private TaskService taskService;

    /**
     * Limpieza automática de tareas cada hora
     */
    @Scheduled(fixedRate = 3600000) // 1 hora
    public void limpiarTareasAutomatico() {
        taskService.limpiarTareasAntiguas();
    }
}
