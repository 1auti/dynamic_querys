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
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/descargar")
@CrossOrigin(origins = "*", maxAge = 3600)
public class DescargarInfraccionesController {

    @Autowired
    private InfraccionesService infraccionesService;

    // =============== ENDPOINTS DE DESCARGA ESPECÍFICOS ===============

    /**
     * Descarga de consulta de personas jurídicas
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<byte[]> descargarPersonasJuridicas(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("personas-juridicas", consulta);
    }

    /**
     * Descarga de reporte general de infracciones
     */
    @PostMapping("/reporte-general")
    public ResponseEntity<byte[]> descargarReporteGeneral(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-general", consulta);
    }

    /**
     * Descarga de reporte de infracciones por equipos
     */
    @PostMapping("/reporte-por-equipos")
    public ResponseEntity<byte[]> descargarReportePorEquipos(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-por-equipos", consulta);
    }

    /**
     * Descarga de reporte de radar fijo
     */
    @PostMapping("/reporte-radar-fijo")
    public ResponseEntity<byte[]> descargarReporteRadarFijo(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-radar-fijo", consulta);
    }

    /**
     * Descarga de reporte de semáforo
     */
    @PostMapping("/reporte-semaforo")
    public ResponseEntity<byte[]> descargarReporteSemaforo(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-semaforo", consulta);
    }

    /**
     * Descarga de vehículos por municipio
     */
    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<byte[]> descargarVehiculosPorMunicipio(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("vehiculos-por-municipio", consulta);
    }

    /**
     * Descarga de reporte sin email por municipio
     */
    @PostMapping("/reporte-sin-email")
    public ResponseEntity<byte[]> descargarReporteSinEmail(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-sin-email", consulta);
    }

    /**
     * Descarga de verificación de imágenes de radar
     */
    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<byte[]> descargarVerificarImagenesRadar(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("verificar-imagenes-radar", consulta);
    }

    /**
     * Descarga de reporte detallado de infracciones
     */
    @PostMapping("/reporte-detallado")
    public ResponseEntity<byte[]> descargarReporteDetallado(
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga("reporte-detallado", consulta);
    }

    // =============== ENDPOINT GENÉRICO ===============

    /**
     * Endpoint genérico para descarga de cualquier tipo de consulta
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarConsultaGenerica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarDescarga(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS DE PRUEBA Y DIAGNÓSTICO ===============

    /**
     * Endpoint de descarga con límites configurables para pruebas
     */
//    @PostMapping("/test/{tipoConsulta}")
//    public ResponseEntity<byte[]> descargarConLimite(
//            @PathVariable String tipoConsulta,
//            @Valid @RequestBody ConsultaQueryDTO consulta,
//            @RequestParam(defaultValue = "5000") int limite,
//            @RequestParam(defaultValue = "json") String formato) {
//
//        log.info("Descarga de prueba - Tipo: {}, Límite: {}, Formato: {}",
//                tipoConsulta, limite, formato);
//
//        // Aplicar límite y formato
//        if (consulta.getParametrosFiltros() == null) {
//            consulta.setParametrosFiltros(new ParametrosFiltrosDTO());
//        }
//
//        consulta.getParametrosFiltros().setLimite(limite);
//        consulta.setFormato(formato);
//
//        return ejecutarDescarga(tipoConsulta, consulta);
//    }

    /**
     * Endpoint para descarga rápida de muestra (solo una provincia)
     */
//    @PostMapping("/muestra/{tipoConsulta}")
//    public ResponseEntity<byte[]> descargarMuestra(
//            @PathVariable String tipoConsulta,
//            @Valid @RequestBody ConsultaQueryDTO consulta,
//            @RequestParam(defaultValue = "1000") int limite) {
//
//        log.info("Descarga de muestra - Tipo: {}, Límite: {}", tipoConsulta, limite);
//
//        // Configurar para muestra rápida
//        if (consulta.getParametrosFiltros() == null) {
//            consulta.setParametrosFiltros(new ParametrosFiltrosDTO());
//        }
//
//        // Forzar una sola provincia para rapidez
//        consulta.getParametrosFiltros().setUsarTodasLasBDS(false);
//        consulta.getParametrosFiltros().setBaseDatos(Arrays.asList("Entre Ríos"));
//        consulta.getParametrosFiltros().setLimite(limite);
//
//        return ejecutarDescarga(tipoConsulta, consulta);
//    }


    // Metodo privada centralizado

    /**
     * Método privado que centraliza la lógica de descarga
     */
    private ResponseEntity<byte[]> ejecutarDescarga(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            log.info("Iniciando descarga - Tipo: {}, Formato: {}",
                    tipoConsulta, consulta.getFormato());

            // Establecer formato por defecto si no se especifica
            if (consulta.getFormato() == null || consulta.getFormato().trim().isEmpty()) {
                consulta.setFormato("json");
                log.debug("Formato establecido por defecto: json");
            }

            ResponseEntity<byte[]> resultado = infraccionesService.descargarConsultaPorTipo(tipoConsulta, consulta);

            log.info("Descarga completada exitosamente - Tipo: {}, Tamaño: {} bytes",
                    tipoConsulta, resultado.getBody() != null ? resultado.getBody().length : 0);

            return resultado;

        } catch (IllegalArgumentException e) {
            log.error("Tipo de consulta no válido para descarga: {}", e.getMessage());
            return ResponseEntity.badRequest()
                    .header("Content-Type", "application/json")
                    .body(crearErrorResponse("Tipo de consulta no válido", e.getMessage()));
        } catch (Exception e) {
            log.error("Error en descarga - Tipo: {}: {}", tipoConsulta, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body(crearErrorResponse("Error interno del servidor", e.getMessage()));
        }
    }

    /**
     * Crea una respuesta de error en formato JSON bytes
     */
    private byte[] crearErrorResponse(String error, String detalle) {
        try {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", error);
            errorResponse.put("detalle", detalle);
            errorResponse.put("timestamp", new Date());

            // Convertir a JSON simple (sin dependencias externas)
            String jsonError = String.format(
                    "{\"error\":\"%s\",\"detalle\":\"%s\",\"timestamp\":\"%s\"}",
                    error.replace("\"", "\\\""),
                    detalle.replace("\"", "\\\""),
                    new Date().toString()
            );

            return jsonError.getBytes("UTF-8");
        } catch (Exception e) {
            String fallback = "{\"error\":\"Error interno\",\"detalle\":\"No se pudo generar respuesta de error\"}";
            return fallback.getBytes();
        }
    }


}