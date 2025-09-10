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
import org.transito_seguro.wrapper.CursorResponseWrapper;

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


    @Value("${app.cursor.default-page-size:50}")
    private int defaultPageSize;

    @Value("${app.cursor.max-page-size:1000}")
    private int maxPageSize;

    @Value("${app.cursor.default-type:fecha_id}")
    private String defaultCursorType;

    // =============== ENDPOINTS PRINCIPALES CON CURSOR ===============

    /**
     * Endpoint genérico principal - SOLO CURSOR
     */
    @PostMapping("/consultar")
    public ResponseEntity<?> consultarInfracciones(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("personas-juridicas", consulta);
    }

    /**
     * Endpoint genérico que maneja todas las consultas específicas - SOLO CURSOR
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsultaEspecifica(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor(tipoConsulta, consulta);
    }

    // =============== ENDPOINTS ESPECÍFICOS CON CURSOR ===============

    /**
     * Endpoint específico para consultar personas jurídicas
     */
    @PostMapping("/personas-juridicas")
    public ResponseEntity<?> consultarPersonasJuridicas(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("personas-juridicas", consulta);
    }

    /**
     * Endpoint para reporte general de infracciones
     */
    @PostMapping("/infracciones-general")
    public ResponseEntity<?> reporteGeneral(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("infracciones-general", consulta);
    }

    /**
     * Endpoint para reporte de infracciones por equipos
     */
    @PostMapping("/infracciones-por-equipos")
    public ResponseEntity<?> reportePorEquipos(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("infracciones-por-equipos", consulta);
    }

    /**
     * Endpoint para reporte de radar fijo
     */
    @PostMapping("/radar-fijo-por-equipo")
    public ResponseEntity<?> reporteRadarFijo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("radar-fijo-por-equipo", consulta);
    }

    /**
     * Endpoint para reporte de semáforo
     */
    @PostMapping("/semaforo-por-equipo")
    public ResponseEntity<?> reporteSemaforo(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("semaforo-por-equipo", consulta);
    }

    /**
     * Endpoint para consultar vehículos por municipio
     */
    @PostMapping("/vehiculos-por-municipio")
    public ResponseEntity<?> vehiculosPorMunicipio(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("vehiculos-por-municipio", consulta);
    }

    /**
     * Endpoint para consultar infracciones sin email por municipio
     */
    @PostMapping("/sin-email-por-municipio")
    public ResponseEntity<?> reporteSinEmail(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("sin-email-por-municipio", consulta);
    }

    /**
     * Endpoint para verificar imágenes de radar
     */
    @PostMapping("/verificar-imagenes-radar")
    public ResponseEntity<?> verificarImagenesRadar(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("verificar-imagenes-radar", consulta);
    }

    /**
     * Endpoint para reporte detallado de infracciones
     */
    @PostMapping("/infracciones-detallado")
    public ResponseEntity<?> reporteDetallado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConCursor("infracciones-detallado", consulta);
    }

    // =============== ENDPOINTS DE NAVEGACIÓN CURSOR SIMPLIFICADOS ===============

    /**
     * Obtener primera página con parámetros simples
     */
    @PostMapping("/{tipoConsulta}/first")
    public ResponseEntity<?> obtenerPrimeraPagina(
            @PathVariable String tipoConsulta,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(defaultValue = "fecha_id") String cursorType,
            @Valid @RequestBody(required = false) ParametrosFiltrosDTO filtros) {

        try {
            pageSize = Math.min(pageSize, maxPageSize); // Aplicar límite

            log.info("Primera página - Tipo: {}, PageSize: {}, Cursor: {}", tipoConsulta, pageSize, cursorType);

            CursorResponseWrapper<Map<String, Object>> response =
                    infraccionesService.obtenerPrimeraPagina(tipoConsulta, cursorType, pageSize, filtros);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error obteniendo primera página: {}", e.getMessage(), e);
            return crearRespuestaError("Error obteniendo primera página", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Obtener siguiente página
     */
    @GetMapping("/{tipoConsulta}/next")
    public ResponseEntity<?> obtenerSiguientePagina(
            @PathVariable String tipoConsulta,
            @RequestParam String cursor,
            @RequestParam(defaultValue = "fecha_id") String cursorType,
            @RequestParam(defaultValue = "50") int pageSize) {

        try {
            pageSize = Math.min(pageSize, maxPageSize); // Aplicar límite

            CursorResponseWrapper<Map<String, Object>> response =
                    infraccionesService.obtenerSiguientePagina(tipoConsulta, cursor, cursorType, pageSize, null);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error obteniendo siguiente página: {}", e.getMessage(), e);
            return crearRespuestaError("Error obteniendo siguiente página", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Obtener página anterior
     */
    @GetMapping("/{tipoConsulta}/prev")
    public ResponseEntity<?> obtenerPaginaAnterior(
            @PathVariable String tipoConsulta,
            @RequestParam String cursor,
            @RequestParam(defaultValue = "fecha_id") String cursorType,
            @RequestParam(defaultValue = "50") int pageSize) {

        try {
            pageSize = Math.min(pageSize, maxPageSize); // Aplicar límite

            CursorResponseWrapper<Map<String, Object>> response =
                    infraccionesService.obtenerPaginaAnterior(tipoConsulta, cursor, cursorType, pageSize, null);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error obteniendo página anterior: {}", e.getMessage(), e);
            return crearRespuestaError("Error obteniendo página anterior", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // =============== ENDPOINTS DE DESCARGA (SIN CAMBIOS MAYORES) ===============

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

    /**
     * Endpoint de descarga alternativo
     */
    @PostMapping("/descargar/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarArchivoAlternativo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta) {
        return descargarArchivo(tipoConsulta, consulta);
    }

    // =============== MÉTODOS PRIVADOS ===============

    /**
     * Método principal que centraliza la ejecución con cursor (reemplaza ejecutarConsultaConLimite)
     */
    private ResponseEntity<?> ejecutarConsultaConCursor(String tipoConsulta, ConsultaQueryDTO consulta) {
        try {
            // Normalizar parámetros de cursor
            ConsultaQueryDTO consultaNormalizada = normalizarConsultaCursor(consulta);

            log.info("Ejecutando consulta tipo: {} - Cursor: {}",
                    tipoConsulta, consultaNormalizada.getParametrosFiltros().getInfoPaginacion());

            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consultaNormalizada);

            // Agregar metadata sobre cursor
            if (resultado instanceof CursorResponseWrapper) {
                return ResponseEntity.ok(resultado);
            } else {
                // Para formatos CSV/Excel, envolver en respuesta simple
                Map<String, Object> respuestaSimple = new HashMap<>();
                respuestaSimple.put("datos", resultado);
                respuestaSimple.put("formato", consultaNormalizada.getFormato());
                respuestaSimple.put("timestamp", new Date());
                return ResponseEntity.ok(respuestaSimple);
            }

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
     * Normaliza parámetros de cursor (reemplaza aplicarLimiteAutomatico)
     */
    private ConsultaQueryDTO normalizarConsultaCursor(ConsultaQueryDTO consultaOriginal) {
        ParametrosFiltrosDTO filtros = consultaOriginal.getParametrosFiltros();

        if (filtros == null) {
            // Crear filtros por defecto con cursor
            filtros = ParametrosFiltrosDTO.primeraPagina(defaultPageSize, defaultCursorType);
        } else {
            // Verificar y normalizar parámetros de cursor
            if (filtros.getTipoCursor() == null) {
                filtros.setTipoCursor(defaultCursorType);
            }

            if (filtros.getPageSize() == null || filtros.getPageSize() <= 0) {
                filtros.setPageSize(defaultPageSize);
                log.info("PageSize no especificado, usando default: {}", defaultPageSize);
            } else if (filtros.getPageSize() > maxPageSize) {
                log.warn("PageSize {} excede el máximo {}, limitando", filtros.getPageSize(), maxPageSize);
                filtros.setPageSize(maxPageSize);
            }

            if (filtros.getDireccion() == null) {
                filtros.setDireccion("forward");
            }

            // Validar parámetros
            if (!filtros.validarPaginacion()) {
                log.warn("Parámetros de cursor inválidos, aplicando defaults");
                ParametrosFiltrosDTO filtrosDefecto = ParametrosFiltrosDTO.primeraPagina(defaultPageSize, defaultCursorType);

                // Mantener filtros de búsqueda originales
                filtros = combinarFiltrosCursor(filtrosDefecto, filtros);
            }
        }

        return ConsultaQueryDTO.builder()
                .formato(consultaOriginal.getFormato() != null ? consultaOriginal.getFormato() : "json")
                .parametrosFiltros(filtros)
                .build();
    }

    /**
     * Combina filtros de cursor con filtros de búsqueda
     */
    private ParametrosFiltrosDTO combinarFiltrosCursor(ParametrosFiltrosDTO filtrosCursor, ParametrosFiltrosDTO filtrosOriginales) {
        return filtrosCursor.toBuilder()
                // Mantener filtros de búsqueda originales
                .fechaInicio(filtrosOriginales.getFechaInicio())
                .fechaFin(filtrosOriginales.getFechaFin())
                .fechaEspecifica(filtrosOriginales.getFechaEspecifica())
                .provincias(filtrosOriginales.getProvincias())
                .municipios(filtrosOriginales.getMunicipios())
                .lugares(filtrosOriginales.getLugares())
                .partido(filtrosOriginales.getPartido())
                .baseDatos(filtrosOriginales.getBaseDatos())
                .patronesEquipos(filtrosOriginales.getPatronesEquipos())
                .tiposDispositivos(filtrosOriginales.getTiposDispositivos())
                .seriesEquiposExactas(filtrosOriginales.getSeriesEquiposExactas())
                .concesiones(filtrosOriginales.getConcesiones())
                .tiposInfracciones(filtrosOriginales.getTiposInfracciones())
                .estadosInfracciones(filtrosOriginales.getEstadosInfracciones())
                .exportadoSacit(filtrosOriginales.getExportadoSacit())
                .tipoVehiculo(filtrosOriginales.getTipoVehiculo())
                .tieneEmail(filtrosOriginales.getTieneEmail())
                .tipoDocumento(filtrosOriginales.getTipoDocumento())
                .usarTodasLasBDS(filtrosOriginales.getUsarTodasLasBDS())
                .filtrosAdicionales(filtrosOriginales.getFiltrosAdicionales())
                .build();
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

    // =============== ENDPOINTS DE INFORMACIÓN ===============

    /**
     * Información sobre configuración de cursor (reemplaza limits-info)
     */
    @GetMapping("/cursor-info")
    public ResponseEntity<?> obtenerInformacionCursor() {
        Map<String, Object> info = new HashMap<>();

        HashMap<String, Object> configuracionCursor = new HashMap<>();
        configuracionCursor.put("page_size_default", defaultPageSize);
        configuracionCursor.put("page_size_maximo", maxPageSize);
        configuracionCursor.put("tipo_cursor_default", defaultCursorType);

        // Tipos de cursor soportados
        info.put("tipos_cursor_soportados", Arrays.asList("id", "fecha", "fecha_id"));

        // Endpoints disponibles
        Map<String, Object> endpoints = new HashMap<>();
        endpoints.put("consulta_generica", "/api/infracciones/{tipoConsulta}");
        endpoints.put("primera_pagina", "/api/infracciones/{tipoConsulta}/first?pageSize=50&cursorType=fecha_id");
        endpoints.put("siguiente_pagina", "/api/infracciones/{tipoConsulta}/next?cursor={cursor}&cursorType=fecha_id&pageSize=50");
        endpoints.put("pagina_anterior", "/api/infracciones/{tipoConsulta}/prev?cursor={cursor}&cursorType=fecha_id&pageSize=50");
        endpoints.put("descarga_archivo", "/api/infracciones/{tipoConsulta}/descargar");

        info.put("endpoints", endpoints);

        // Ejemplo de request
        Map<String, Object> ejemploRequest = new HashMap<>();
        ejemploRequest.put("formato", "json");
        HashMap<String, Object> parametrosFiltros = new HashMap<>();
        parametrosFiltros.put("cursor", "2024-01-15T10:30:00|12345");
        parametrosFiltros.put("tipoCursor", "fecha_id");
        parametrosFiltros.put("pageSize", 50);
        parametrosFiltros.put("direccion", "forward");
        parametrosFiltros.put("fechaInicio", "2024-01-01");
        parametrosFiltros.put("municipios", Arrays.asList("Buenos Aires", "Avellaneda"));

        ejemploRequest.put("parametrosFiltros", parametrosFiltros);

        info.put("ejemplo_request_body", ejemploRequest);

        // Ventajas del cursor
        info.put("ventajas_cursor", Arrays.asList(
                "Performance constante O(log n) vs O(n) del offset",
                "Funciona igual de rápido en página 1 o 10,000",
                "No afectado por inserciones/eliminaciones concurrentes",
                "Ideal para scroll infinito y aplicaciones en tiempo real",
                "Menor uso de memoria - solo procesa la página solicitada"
        ));

        info.put("timestamp", new Date());

        return ResponseEntity.ok(info);
    }

    /**
     * Endpoint para obtener tipos de consulta disponibles
     */
    @GetMapping("/tipos-consulta")
    public ResponseEntity<?> obtenerTiposConsulta() {
        Map<String, Object> info = new HashMap<>();

        info.put("tipos_disponibles", Arrays.asList(
                "personas-juridicas",
                "infracciones-general",
                "infracciones-por-equipos",
                "radar-fijo-por-equipo",
                "semaforo-por-equipo",
                "vehiculos-por-municipio",
                "sin-email-por-municipio",
                "verificar-imagenes-radar",
                "infracciones-detallado"
        ));

        info.put("formato_endpoint", "/api/infracciones/{tipoConsulta}");
        info.put("formatos_soportados", Arrays.asList("json", "csv", "excel"));

        return ResponseEntity.ok(info);
    }

    /**
     * Endpoint de prueba para verificar configuración
     */
    @GetMapping("/health")
    public ResponseEntity<?> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "OK");

        Map<String, Object> cursorConfig = new HashMap<>();
        cursorConfig.put("default_page_size", defaultPageSize);
        cursorConfig.put("max_page_size", maxPageSize);
        cursorConfig.put("default_cursor_type", defaultCursorType);
        health.put("cursor_config", cursorConfig);

        health.put("timestamp", new Date());
        return ResponseEntity.ok(health);
    }
}