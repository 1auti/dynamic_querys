package org.transito_seguro.controller;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/monitor")
public class DataSourceMonitorController {

    private final HikariDataSource pbaDataSource;
    private final HikariDataSource mdaDataSource;
    private final HikariDataSource santaRosaDataSource;
    private final HikariDataSource chacoDataSource;
    private final HikariDataSource entreRiosDataSource;
    private final HikariDataSource formosaDataSource;

    public DataSourceMonitorController(
            @Qualifier("pbaDataSource") DataSource pbaDs,
            @Qualifier("mdaDataSource") DataSource mdaDs,
            @Qualifier("santa-rosaDataSource") DataSource santaRosaDs,
            @Qualifier("chacoDataSource") DataSource chacoDs,
            @Qualifier("entre-riosDataSource") DataSource entreRiosDs,
            @Qualifier("formosaDataSource") DataSource formosaDs) {

        this.pbaDataSource = (HikariDataSource) pbaDs;
        this.mdaDataSource = (HikariDataSource) mdaDs;
        this.santaRosaDataSource = (HikariDataSource) santaRosaDs;
        this.chacoDataSource = (HikariDataSource) chacoDs;
        this.entreRiosDataSource = (HikariDataSource) entreRiosDs;
        this.formosaDataSource = (HikariDataSource) formosaDs;
    }

    @GetMapping("/pools")
    public Map<String, Object> getPoolStats() {
        Map<String, Object> stats = new LinkedHashMap<>();

        stats.put("Buenos Aires", getPoolInfo(pbaDataSource));
        stats.put("Avellaneda", getPoolInfo(mdaDataSource));
        stats.put("La Pampa", getPoolInfo(santaRosaDataSource));
        stats.put("Chaco", getPoolInfo(chacoDataSource));
        stats.put("Entre Ríos", getPoolInfo(entreRiosDataSource));
        stats.put("Formosa", getPoolInfo(formosaDataSource));

        // Totales
        Map<String, Object> totales = new LinkedHashMap<>();
        totales.put("total_active", stats.values().stream()
                .mapToInt(v -> (Integer) ((Map) v).get("active"))
                .sum());
        totales.put("total_idle", stats.values().stream()
                .mapToInt(v -> (Integer) ((Map) v).get("idle"))
                .sum());
        totales.put("total_waiting", stats.values().stream()
                .mapToInt(v -> (Integer) ((Map) v).get("waiting"))
                .sum());

        stats.put("_totales", totales);

        return stats;
    }

    private Map<String, Object> getPoolInfo(HikariDataSource ds) {
        Map<String, Object> info = new LinkedHashMap<>();
        try {
            HikariPoolMXBean pool = ds.getHikariPoolMXBean();
            info.put("active", pool.getActiveConnections());
            info.put("idle", pool.getIdleConnections());
            info.put("total", pool.getTotalConnections());
            info.put("waiting", pool.getThreadsAwaitingConnection());
            info.put("max", ds.getMaximumPoolSize());
        } catch (Exception e) {
            info.put("error", e.getMessage());
        }
        return info;
    }
}