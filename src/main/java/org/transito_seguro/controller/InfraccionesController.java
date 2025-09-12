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

    @Value("${app.limits.max-records-display:5000}")
    private int maxRecordsDisplay;

    // =============== ENDPOINTS GENÉRICOS (MANEJAN CONSOLIDACIÓN AUTOMÁTICAMENTE) ===============

    /**
     * Endpoint genérico para consultar infracciones
     * MANEJA AUTOMÁTICAMENTE consolidación si consolidado=true en los filtros
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("personas-juridicas", consulta);
    }

    /**
     * Endpoint genérico que maneja todas las consultas específicas
     * MANEJA AUTOMÁTICAMENTE consolidación si consolidado=true en los filtros
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsultaEspecifica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite(tipoConsulta, consulta);
    }

    /**
     * Endpoint genérico para descarga de archivos
     * MANEJA AUTOMÁTICAMENTE consolidación si consolidado=true en los filtros
     */
    @PostMapping("/{tipoConsulta}/descargar")
    public ResponseEntity<byte[]> descargarArchivo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        try {
            log.info("Descargando archivo para tipo: {} - Consolidado: {}",
                    tipoConsulta,
                    consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado());
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
     * Endpoint de descarga alternativo
     */
    @PostMapping("/descargar/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarArchivoAlternativo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return descargarArchivo(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS ESPECÍFICOS (MANTENER COMPATIBILIDAD) ===============

    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("personas-juridicas", consulta);
    }

    @PostMapping("/infracciones-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("infracciones-general", consulta);
    }

    @PostMapping("/infracciones-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("infracciones-por-equipos", consulta);
    }

    @PostMapping("/radar-fijo-por-equipo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("radar-fijo-por-equipo", consulta);
    }

    @PostMapping("/semaforo-por-equipo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("semaforo-por-equipo", consulta);
    }

    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("vehiculos-por-municipio", consulta);
    }


    @PostMapping("/sin-email-por-municipio")
    public ResponseEntity<?> reporteSinEmail(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("sin-email-por-municipio", consulta);
    }

    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<?> verificarImagenesRadar(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("verificar-imagenes-radar", consulta);
    }

    @PostMapping("/infracciones-detallado")
    public ResponseEntity<?> reporteDetallado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("infracciones-detallado", consulta);
    }

    @PostMapping("/infracciones-por-estado")
    public ResponseEntity<?> infraccionesPorEstado(@Valid @RequestBody ConsultaQueryDTO consulta){
        return ejecutarConsultaConLimite("infracciones-por-estado",consulta);
    }


    // =============== MÉTODOS PRIVADOS ===============

    /**
     * Método que aplica límite automático y centraliza el manejo de errores
     * DETECTA AUTOMÁTICAMENTE si es consolidación y ajusta la respuesta
     */
    private ResponseEntity<?> ejecutarConsultaConLimite(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            // Aplicar límite automático
            ConsultaQueryDTO consultaConLimite = aplicarLimiteAutomatico(consulta);

            boolean esConsolidado = consultaConLimite.getParametrosFiltros() != null &&
                    consultaConLimite.getParametrosFiltros().esConsolidado();

            log.info("Ejecutando consulta tipo: {} - LÍMITE: {} - CONSOLIDADO: {}",
                    tipoConsulta,
                    consultaConLimite.getParametrosFiltros().getLimiteEfectivo(),
                    esConsolidado);

            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consultaConLimite);

            // Crear respuesta con metadata apropiada
            Map<String, Object> respuestaConMetadata = esConsolidado ?
                    crearRespuestaConsolidadaConMetadata(resultado, consulta, consultaConLimite, tipoConsulta) :
                    crearRespuestaConMetadata(resultado, consulta, consultaConLimite);

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
            filtrosOriginales = ParametrosFiltrosDTO.builder()
                    .limite(maxRecordsDisplay)
                    .build();
        } else {
            int limiteActual = filtrosOriginales.getLimiteEfectivo();
            boolean limiteFueAplicado = false;

            if (limiteActual > maxRecordsDisplay) {
                log.info("Aplicando límite automático: {} -> {} registros", limiteActual, maxRecordsDisplay);
                limiteFueAplicado = true;

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
     * Crea respuesta con metadata sobre límites aplicados (consultas normales)
     */
    private Map<String, Object> crearRespuestaConMetadata(Object resultado,
                                                          ConsultaQueryDTO consultaOriginal,
                                                          ConsultaQueryDTO consultaConLimite) {
        Map<String, Object> respuesta = new HashMap<>();
        respuesta.put("datos", resultado);

        Map<String, Object> metadata = new HashMap<>();

        int limiteSolicitado = consultaOriginal.getParametrosFiltros() != null ?
                consultaOriginal.getParametrosFiltros().getLimiteEfectivo() : maxRecordsDisplay;
        int limiteAplicado = consultaConLimite.getParametrosFiltros().getLimiteEfectivo();

        metadata.put("limite_solicitado", limiteSolicitado);
        metadata.put("limite_aplicado", limiteAplicado);
        metadata.put("limite_fue_reducido", limiteSolicitado > limiteAplicado);
        metadata.put("limite_maximo_consultas", maxRecordsDisplay);
        metadata.put("tipo_consulta", "normal");

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
     * Crea respuesta con metadata específica para consolidación
     */
    private Map<String, Object> crearRespuestaConsolidadaConMetadata(Object resultado,
                                                                     ConsultaQueryDTO consultaOriginal,
                                                                     ConsultaQueryDTO consultaConLimite,
                                                                     String tipoConsulta) {
        Map<String, Object> respuesta = new HashMap<>();
        respuesta.put("datos", resultado);

        Map<String, Object> metadata = new HashMap<>();

        int limiteSolicitado = consultaOriginal.getParametrosFiltros() != null ?
                consultaOriginal.getParametrosFiltros().getLimiteEfectivo() : maxRecordsDisplay;
        int limiteAplicado = consultaConLimite.getParametrosFiltros().getLimiteEfectivo();

        metadata.put("limite_solicitado", limiteSolicitado);
        metadata.put("limite_aplicado", limiteAplicado);
        metadata.put("limite_fue_reducido", limiteSolicitado > limiteAplicado);
        metadata.put("limite_maximo_consultas", maxRecordsDisplay);
        metadata.put("tipo_consulta", "consolidada");
        metadata.put("consolidacion_activa", true);

        // Información específica de consolidación
        try {
            Map<String, Object> infoConsolidacion = infraccionesService.obtenerInfoConsolidacion();
            metadata.put("provincias_disponibles", infoConsolidacion.get("provincias_disponibles"));
            metadata.put("total_provincias_consultadas", infoConsolidacion.get("total_provincias"));
        } catch (Exception e) {
            log.warn("No se pudo obtener info de consolidación para metadata: {}", e.getMessage());
        }

        if (limiteSolicitado > limiteAplicado) {
            metadata.put("mensaje", String.format(
                    "Su consulta CONSOLIDADA fue limitada a %d registros por provincia. " +
                            "Para obtener datos completos consolidados, use '/descargar' que genera archivos sin límite.",
                    limiteAplicado
            ));
            metadata.put("endpoint_descarga_sugerido", "/api/infracciones/" + tipoConsulta + "/descargar");
        } else {
            metadata.put("mensaje", "Consulta consolidada ejecutada exitosamente. Los datos incluyen información de todas las provincias disponibles.");
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

}