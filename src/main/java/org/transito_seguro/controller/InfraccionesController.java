package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.service.InfraccionesService;

import javax.validation.Valid;
import javax.xml.bind.ValidationException;
import java.util.*;

/**
 * Controller para endpoints tradicionales de infracciones
 * Se enfoca en consultas basadas en archivos SQL estáticos
 * Para queries dinámicas usar DatabaseQueryController
 */
@Slf4j
@RestController
@RequestMapping("/api/infracciones")
@CrossOrigin(origins = "*", maxAge = 3600)
public class InfraccionesController {

    @Autowired
    private InfraccionesService infraccionesService;

    @Value("${app.limits.max-records-display:5000}")
    private int maxRecordsDisplay;

    // =============== ENDPOINTS PRINCIPALES ===============

    /**
     * Endpoint principal genérico - maneja todas las consultas
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsulta(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        return procesarConsulta(tipoConsulta, consulta);
    }

    /**
     * Endpoint legacy para consultar infracciones
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("personas-juridicas", consulta);
    }

    /**
     * Endpoint de descarga de archivos
     */
    @PostMapping("/{tipoConsulta}/descargar")
    public ResponseEntity<byte[]> descargarArchivo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        try {
            log.info("Descarga - Tipo: {}, Consolidado: {}",
                    tipoConsulta, esConsolidado(consulta));

            return infraccionesService.descargarConsultaPorTipo(tipoConsulta, consulta);

        } catch (IllegalArgumentException e) {
            log.error("Tipo no válido para descarga: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error en descarga {}: {}", tipoConsulta, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Endpoint alternativo de descarga
     */
    @PostMapping("/descargar/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarArchivoAlternativo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return descargarArchivo(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS ESPECÍFICOS (COMPATIBILIDAD) ===============

    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("personas-juridicas", consulta);
    }

    @PostMapping("/infracciones-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("infracciones-general", consulta);
    }

    @PostMapping("/infracciones-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("infracciones-por-equipos", consulta);
    }

    @PostMapping("/radar-fijo-por-equipo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("radar-fijo-por-equipo", consulta);
    }

    @PostMapping("/semaforo-por-equipo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("semaforo-por-equipo", consulta);
    }

    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("vehiculos-por-municipio", consulta);
    }

    @PostMapping("/sin-email-por-municipio")
    public ResponseEntity<?> reporteSinEmail(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("sin-email-por-municipio", consulta);
    }

    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<?> verificarImagenesRadar(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("verificar-imagenes-radar", consulta);
    }

    @PostMapping("/infracciones-detallado")
    public ResponseEntity<?> reporteDetallado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("infracciones-detallado", consulta);
    }

    @PostMapping("/infracciones-por-estado")
    public ResponseEntity<?> infraccionesPorEstado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("infracciones-por-estado", consulta);
    }

    @PostMapping("/reporte-diego")
    public ResponseEntity<?> reporteDiego(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return procesarConsulta("reporte-diego", consulta);
    }

    // =============== MÉTODOS PRIVADOS ===============

    /**
     * Método centralizado para procesar consultas
     */
    private ResponseEntity<?> procesarConsulta(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            // Aplicar límite automático
            ConsultaQueryDTO consultaConLimite = aplicarLimiteAutomatico(consulta);
            boolean consolidado = esConsolidado(consultaConLimite);

            log.info("Procesando: {} - Límite: {} - Consolidado: {}",
                    tipoConsulta,
                    consultaConLimite.getParametrosFiltros().getLimiteEfectivo(),
                    consolidado);

            // Ejecutar consulta
            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consultaConLimite);

            // Preparar respuesta
            ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok();

            if (consolidado) {
                responseBuilder
                        .header("X-Consolidado", "true")
                        .header("X-Tipo-Consulta", tipoConsulta);
            }

            return responseBuilder.body(resultado);

        } catch (ValidationException e) {
            log.error("Validación fallida en {}: {}", tipoConsulta, e.getMessage());
            return crearRespuestaError("Error de validación", e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (IllegalArgumentException e) {
            log.error("Tipo de consulta no válido: {}", e.getMessage());
            return crearRespuestaError("Consulta no soportada", e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("Error interno en {}: {}", tipoConsulta, e.getMessage(), e);
            return crearRespuestaError("Error interno", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Aplica límite automático
     */
    private ConsultaQueryDTO aplicarLimiteAutomatico(ConsultaQueryDTO consulta) {
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        if (filtros == null) {
            filtros = ParametrosFiltrosDTO.builder()
                    .limite(maxRecordsDisplay)
                    .build();
        } else {
            int limite = filtros.getLimiteEfectivo();
            if (limite > maxRecordsDisplay) {
                log.info("Aplicando límite: {} -> {}", limite, maxRecordsDisplay);
                filtros = filtros.toBuilder()
                        .limite(maxRecordsDisplay)
                        .build();
            }
        }

        return ConsultaQueryDTO.builder()
                .formato(consulta.getFormato())
                .parametrosFiltros(filtros)
                .build();
    }

    /**
     * Verifica si es consolidado
     */
    private boolean esConsolidado(ConsultaQueryDTO consulta) {
        return consulta.getParametrosFiltros() != null &&
                consulta.getParametrosFiltros().esConsolidado();
    }

    /**
     * Crea respuesta de error estándar
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