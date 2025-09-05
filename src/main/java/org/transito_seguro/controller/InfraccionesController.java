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

    // =============== ENDPOINTS GENÉRICOS ===============

    /**
     * Endpoint genérico para consultar infracciones con filtros dinámicos
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("personas-juridicas", consulta);
    }

    /**
     * Endpoint genérico que maneja todas las consultas específicas
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsultaEspecifica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta(tipoConsulta, consulta);
    }

    /**
     * Endpoint genérico para descarga de archivos
     */
    @PostMapping("/{tipoConsulta}/descargar")
    public ResponseEntity<byte[]> descargarArchivo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            log.info("Descargando archivo para tipo: {}", tipoConsulta);
            return infraccionesService.descargarConsultaPorTipo(tipoConsulta, consulta);
        } catch (IllegalArgumentException e) {
            log.error("Tipo de consulta no válido: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error descargando archivo para tipo {}: {}", tipoConsulta, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // =============== ENDPOINTS ESPECÍFICOS (MANTENER COMPATIBILIDAD) ===============

    /**
     * Endpoint específico para consultar personas jurídicas
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("personas-juridicas", consulta);
    }

    /**
     * Endpoint para reporte general de infracciones
     */
    @PostMapping("/reporte-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-general", consulta);
    }

    /**
     * Endpoint para reporte de infracciones por equipos
     */
    @PostMapping("/reporte-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-por-equipos", consulta);
    }

    /**
     * Endpoint para reporte de radar fijo
     */
    @PostMapping("/reporte-radar-fijo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-radar-fijo", consulta);
    }

    /**
     * Endpoint para reporte de semáforo
     */
    @PostMapping("/reporte-semaforo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-semaforo", consulta);
    }

    /**
     * Endpoint para consultar vehículos por municipio
     */
    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("vehiculos-por-municipio", consulta);
    }

    /**
     * Endpoint para consultar infracciones sin email por municipio
     */
    @PostMapping("/reporte-sin-email")
    public ResponseEntity<?> reporteSinEmail(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-sin-email", consulta);
    }

    /**
     * Endpoint para verificar imágenes de radar
     */
    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<?> verificarImagenesRadar(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("verificar-imagenes-radar", consulta);
    }

    /**
     * Endpoint para reporte detallado de infracciones
     */
    @PostMapping("/reporte-detallado")
    public ResponseEntity<?> reporteDetallado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsulta("reporte-detallado", consulta);
    }



    // =============== MÉTODO PRIVADO CENTRALIZADO ===============

    /**
     * Método privado que centraliza el manejo de errores para todas las consultas
     */
    private ResponseEntity<?> ejecutarConsulta(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            log.info("Ejecutando consulta tipo: {}", tipoConsulta);
            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consulta);
            return ResponseEntity.ok(resultado);
        } catch (ValidationException e) {
            log.error("Error de validación en consulta {}: {}", tipoConsulta, e.getMessage());
            return crearRespuestaError("Validación fallida", e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (IllegalArgumentException e) {
            log.error("Tipo de consulta no válido: {}", e.getMessage());
            return crearRespuestaError("Tipo de consulta no soportado", e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("Error interno en consulta {}: {}", tipoConsulta, e.getMessage(), e);
            return crearRespuestaError("Error interno del servidor", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Método utilitario para crear respuestas de error consistentes
     */
    private ResponseEntity<?> crearRespuestaError(String error, String detalle, HttpStatus status) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", error);
        errorResponse.put("detalle", detalle);
        errorResponse.put("timestamp", new Date());
        errorResponse.put("status", status.value());
        return ResponseEntity.status(status).body(errorResponse);
    }
}