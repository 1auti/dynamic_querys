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

@Slf4j
@RestController
@RequestMapping("/api/infracciones")
@CrossOrigin(origins = "*", maxAge = 3600)
public class InfraccionesController {

    @Autowired
    private InfraccionesService infraccionesService;

    // LÍMITE MÁXIMO PARA ENDPOINTS DE CONSULTA (no descarga)
    @Value("${app.limits.max-records-display:5000}")
    private int maxRecordsDisplay;

    // =============== ENDPOINTS GENÉRICOS CON LÍMITE ===============

    /**
     * Endpoint genérico para consultar infracciones con límite automático
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("personas-juridicas", consulta);
    }

    /**
     * Endpoint genérico que maneja todas las consultas específicas CON LÍMITE
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsultaEspecifica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS DE DESCARGA SIN LÍMITE ===============

    /**
     * Endpoint genérico para descarga de archivos SIN LÍMITE
     */
    @PostMapping("/{tipoConsulta}/descargar")
    public ResponseEntity<byte[]> descargarArchivo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            log.info("Descargando archivo para tipo: {} - SIN LÍMITE", tipoConsulta);
            return infraccionesService.descargarConsultaPorTipo(tipoConsulta, consulta);
        } catch (IllegalArgumentException e) {
            log.error("Tipo de consulta no válido: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error descargando archivo para tipo {}: {}", tipoConsulta, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Endpoint de descarga genérico alternativo
     */
    @PostMapping("/descargar/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarArchivoAlternativo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return descargarArchivo(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS ESPECÍFICOS CON LÍMITE (MANTENER COMPATIBILIDAD) ===============

    /**
     * Endpoint específico para consultar personas jurídicas CON LÍMITE
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("personas-juridicas", consulta);
    }

    /**
     * Endpoint para reporte general de infracciones CON LÍMITE
     */
    @PostMapping("/reporte-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-general", consulta);
    }

    /**
     * Endpoint para reporte de infracciones por equipos CON LÍMITE
     */
    @PostMapping("/reporte-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-por-equipos", consulta);
    }

    /**
     * Endpoint para reporte de radar fijo CON LÍMITE
     */
    @PostMapping("/reporte-radar-fijo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-radar-fijo", consulta);
    }

    /**
     * Endpoint para reporte de semáforo CON LÍMITE
     */
    @PostMapping("/reporte-semaforo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-semaforo", consulta);
    }

    /**
     * Endpoint para consultar vehículos por municipio CON LÍMITE
     */
    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("vehiculos-por-municipio", consulta);
    }

    /**
     * Endpoint para consultar infracciones sin email por municipio CON LÍMITE
     */
    @PostMapping("/reporte-sin-email")
    public ResponseEntity<?> reporteSinEmail(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-sin-email", consulta);
    }

    /**
     * Endpoint para verificar imágenes de radar CON LÍMITE
     */
    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<?> verificarImagenesRadar(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("verificar-imagenes-radar", consulta);
    }

    /**
     * Endpoint para reporte detallado de infracciones CON LÍMITE
     */
    @PostMapping("/reporte-detallado")
    public ResponseEntity<?> reporteDetallado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-detallado", consulta);
    }

    // =============== MÉTODOS PRIVADOS ===============

    /**
     * Método que aplica límite automático y centraliza el manejo de errores
     */
    private ResponseEntity<?> ejecutarConsultaConLimite(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            // APLICAR LÍMITE AUTOMÁTICO
            ConsultaQueryDTO consultaConLimite = aplicarLimiteAutomatico(consulta);

            log.info("Ejecutando consulta tipo: {} - LÍMITE APLICADO: {}",
                    tipoConsulta, consultaConLimite.getParametrosFiltros().getLimiteEfectivo());

            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consultaConLimite);

            // Agregar metadata sobre el límite aplicado
            Map<String, Object> respuestaConMetadata = crearRespuestaConMetadata(resultado, consulta, consultaConLimite);

            return ResponseEntity.ok(respuestaConMetadata);

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
     * Aplica límite máximo automáticamente a la consulta
     */
    private ConsultaQueryDTO aplicarLimiteAutomatico(ConsultaQueryDTO consultaOriginal) {
        ParametrosFiltrosDTO filtrosOriginales = consultaOriginal.getParametrosFiltros();

        if (filtrosOriginales == null) {
            // Crear filtros por defecto con límite
            filtrosOriginales = ParametrosFiltrosDTO.builder()
                    .limite(maxRecordsDisplay)
                    .build();
        } else {
            // Verificar si necesita aplicar límite
            int limiteActual = filtrosOriginales.getLimiteEfectivo();
            boolean limiteFueAplicado = false;

            if (limiteActual > maxRecordsDisplay) {
                log.info("Aplicando límite automático: {} -> {} registros", limiteActual, maxRecordsDisplay);
                limiteFueAplicado = true;

                // Crear nuevos filtros con límite aplicado
                filtrosOriginales = filtrosOriginales.toBuilder()
                        .limite(maxRecordsDisplay)
                        .limiteMaximo(maxRecordsDisplay)
                        .build();
            }

            if (limiteFueAplicado) {
                log.warn("LÍMITE APLICADO: El límite solicitado excedía el máximo permitido para consultas ({}). Use /descargar para obtener más registros.", maxRecordsDisplay);
            }
        }

        return ConsultaQueryDTO.builder()
                .formato(consultaOriginal.getFormato())
                .parametrosFiltros(filtrosOriginales)
                .build();
    }

    /**
     * Crea respuesta con metadata sobre límites aplicados
     */
    private Map<String, Object> crearRespuestaConMetadata(Object resultado,
                                                          ConsultaQueryDTO consultaOriginal,
                                                          ConsultaQueryDTO consultaConLimite) {
        Map<String, Object> respuesta = new HashMap<>();

        // Resultado principal
        respuesta.put("datos", resultado);

        // Metadata sobre límites
        Map<String, Object> metadata = new HashMap<>();

        int limiteSolicitado = consultaOriginal.getParametrosFiltros() != null ?
                consultaOriginal.getParametrosFiltros().getLimiteEfectivo() : maxRecordsDisplay;
        int limiteAplicado = consultaConLimite.getParametrosFiltros().getLimiteEfectivo();

        metadata.put("limite_solicitado", limiteSolicitado);
        metadata.put("limite_aplicado", limiteAplicado);
        metadata.put("limite_fue_reducido", limiteSolicitado > limiteAplicado);
        metadata.put("limite_maximo_consultas", maxRecordsDisplay);

        if (limiteSolicitado > limiteAplicado) {
            metadata.put("mensaje", String.format(
                    "Su consulta fue limitada a %d registros para optimizar la respuesta. " +
                            "Para obtener más datos, use los endpoints '/descargar' que generan archivos sin límite.",
                    limiteAplicado
            ));
            metadata.put("endpoint_descarga_sugerido", "/api/infracciones/{tipoConsulta}/descargar");
        }

        respuesta.put("metadata", metadata);
        respuesta.put("timestamp", new Date());

        return respuesta;
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

    // =============== ENDPOINT DE INFORMACIÓN ===============

    /**
     * Endpoint para obtener información sobre límites
     */
    @GetMapping("/limits-info")
    public ResponseEntity<?> obtenerInformacionLimites() {
        Map<String, Object> info = new HashMap<>();
        info.put("limite_maximo_consultas", maxRecordsDisplay);
        info.put("limite_maximo_descargas", "Sin límite");
        info.put("mensaje", "Los endpoints de consulta están limitados a " + maxRecordsDisplay +
                " registros. Use /descargar para obtener archivos completos sin límite.");
        info.put("endpoints_sin_limite", Arrays.asList(
                "/api/infracciones/{tipoConsulta}/descargar",
                "/api/infracciones/descargar/{tipoConsulta}"
        ));
        return ResponseEntity.ok(info);
    }
}