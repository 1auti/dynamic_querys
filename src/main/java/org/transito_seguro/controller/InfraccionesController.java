package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.service.InfraccionesService;

import javax.validation.Valid;
import javax.xml.bind.ValidationException;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/infracciones")
@CrossOrigin(origins = "*", maxAge = 3600)
public class InfraccionesController {

    @Autowired
    private InfraccionesService infraccionesService;

    /**
     * Endpoint genérico para consultar infracciones con filtros dinámicos
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            log.info("Recibida consulta de infracciones: {}", consulta);
            Object resultado = infraccionesService.consultarInfracciones(consulta);
            return ResponseEntity.ok(resultado);
        } catch (ValidationException e) {
            log.error("Error de validación en consulta: {}", e.getMessage());
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validación fallida");
            errorResponse.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(errorResponse);
        } catch (Exception e) {
            log.error("Error interno en consulta de infracciones", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error interno del servidor");
            errorResponse.put("detalle", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint específico para consultar personas jurídicas
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarPersonasJuridicas(consulta);
            return ResponseEntity.ok(resultado);
        } catch (ValidationException e) {
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validación fallida");
            errorResponse.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(errorResponse);
        } catch (Exception e) {
            log.error("Error en consulta personas jurídicas", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error interno del servidor");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para reporte general de infracciones
     */
    @PostMapping("/reporte-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarReporteGeneral(consulta);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            log.error("Error en reporte general", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error generando reporte");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para reporte de infracciones por equipos
     */
    @PostMapping("/reporte-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarInfraccionesPorEquipos(consulta);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            log.error("Error en reporte por equipos", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error generando reporte por equipos");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para reporte de radar fijo
     */
    @PostMapping("/reporte-radar-fijo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarRadarFijoPorEquipo(consulta);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            log.error("Error en reporte radar fijo", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error generando reporte radar fijo");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para reporte de semáforo
     */
    @PostMapping("/reporte-semaforo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarSemaforoPorEquipo(consulta);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            log.error("Error en reporte semáforo", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error generando reporte semáforo");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para consultar vehículos por municipio
     */
    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            Object resultado = infraccionesService.consultarVehiculosPorMunicipio(consulta);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            log.error("Error en consulta vehículos por municipio", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error consultando vehículos");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }





}