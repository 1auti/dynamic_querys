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
import java.math.BigDecimal;
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

    // =================== ENDPOINTS DE UTILIDAD ===================

    /**
     * Endpoint para obtener estadísticas de la consulta
     */
    @PostMapping("/estadisticas")
    public ResponseEntity<?> obtenerEstadisticas(@Valid @RequestBody ParametrosFiltrosDTO filtros) {
        try {
            Map<String, Object> estadisticas = infraccionesService.obtenerEstadisticasConsulta(filtros);
            return ResponseEntity.ok(estadisticas);
        } catch (Exception e) {
            log.error("Error obteniendo estadísticas", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error obteniendo estadísticas");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para validar conectividad de repositorios
     */
    @GetMapping("/salud")
    public ResponseEntity<?> validarSalud() {
        try {
            Map<String, Boolean> conectividad = infraccionesService.validarConectividadRepositorios();
            boolean todasConectadas = conectividad.values().stream().allMatch(Boolean::booleanValue);
            HttpStatus status = todasConectadas ? HttpStatus.OK : HttpStatus.PARTIAL_CONTENT;
            HashMap<String, Object> response = new HashMap<>();
            response.put("estado", todasConectadas ? "SALUDABLE" : "PARCIAL");
            response.put("repositorios", conectividad);
            response.put("timestamp", new Date());
            return ResponseEntity.status(status).body(response);
        } catch (Exception e) {
            log.error("Error validando salud del sistema", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error validando salud del sistema");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorResponse);
        }
    }

    // =================== ENDPOINTS DE CONSULTA RÁPIDA ===================

    /**
     * Endpoint GET para consultas rápidas (con parámetros en URL)
     */
    @GetMapping("/consulta-rapida")
    public ResponseEntity<?> consultaRapida(
            @RequestParam(required = false) String provincia,
            @RequestParam(required = false) String municipio,
            @RequestParam(required = false) String fechaInicio,
            @RequestParam(required = false) String fechaFin,
            @RequestParam(required = false, defaultValue = "json") String formato,
            @RequestParam(required = false, defaultValue = "100") Integer limite) {

        try {
            // Construir DTO desde parámetros URL
            ParametrosFiltrosDTO.ParametrosFiltrosDTOBuilder filtrosBuilder = ParametrosFiltrosDTO.builder()
                    .limiteMaximo(limite);

            if (provincia != null) {
                filtrosBuilder.baseDatos(Arrays.asList(provincia));
                filtrosBuilder.usarTodasLasBDS(false);
            }

            if (municipio != null) {
                filtrosBuilder.municipios(Arrays.asList(municipio));
            }

            // TODO: Parsear fechas desde strings

            ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                    .formato(formato)
                    .parametrosFiltros(filtrosBuilder.build())
                    .build();

            Object resultado = infraccionesService.consultarInfracciones(consulta);
            return ResponseEntity.ok(resultado);

        } catch (Exception e) {
            log.error("Error en consulta rápida", e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error en consulta rápida");
            errorResponse.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    /**
     * Endpoint para obtener info de dominios específicos
     */
    @GetMapping("/dominio/{dominio}")
    public ResponseEntity<?> consultarPorDominio(
            @PathVariable String dominio,
            @RequestParam(required = false, defaultValue = "json") String formato) {

        try {
            ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                    .formato(formato)
                    .parametrosFiltros(
                            ParametrosFiltrosDTO.builder()
                                    .dominios(Arrays.asList(dominio.toUpperCase()))
                                    .usarTodasLasBDS(true)
                                    .build()
                    )
                    .build();

            Object resultado = infraccionesService.consultarPersonasJuridicas(consulta);
            return ResponseEntity.ok(resultado);

        } catch (Exception e) {
            log.error("Error consultando dominio: {}", dominio, e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error consultando dominio");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Endpoint para consultar por documento
     */
    @GetMapping("/documento/{numeroDocumento}")
    public ResponseEntity<?> consultarPorDocumento(
            @PathVariable String numeroDocumento,
            @RequestParam(required = false, defaultValue = "json") String formato) {

        try {
            ConsultaQueryDTO consulta = ConsultaQueryDTO.builder()
                    .formato(formato)
                    .parametrosFiltros(
                            ParametrosFiltrosDTO.builder()
                                    .numerosDocumentos(Arrays.asList(numeroDocumento))
                                    .usarTodasLasBDS(true)
                                    .build()
                    )
                    .build();

            Object resultado = infraccionesService.consultarPersonasJuridicas(consulta);
            return ResponseEntity.ok(resultado);

        } catch (Exception e) {
            log.error("Error consultando documento: {}", numeroDocumento, e);
            HashMap<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error consultando documento");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    // =================== ENDPOINT DE AYUDA ===================

    /**
     * Endpoint para obtener información sobre filtros disponibles
     */
    @GetMapping("/ayuda/filtros")
    public ResponseEntity<?> obtenerAyudaFiltros() {
        HashMap<String, Object> ayuda = new HashMap<>();

        // Sección fechas
        HashMap<String, Object> fechas = new HashMap<>();
        fechas.put("fechaEspecifica", "Fecha específica (yyyy-MM-dd)");
        fechas.put("fechaInicio", "Fecha inicio del rango (yyyy-MM-dd)");
        fechas.put("fechaFin", "Fecha fin del rango (yyyy-MM-dd)");
        fechas.put("usarFechaEspecifica", "Boolean para usar fecha específica en lugar de rango");
        ayuda.put("fechas", fechas);

        // Sección ubicación
        HashMap<String, Object> ubicacion = new HashMap<>();
        ubicacion.put("baseDatos", "Lista de provincias: [pba, mda, santa_rosa, chaco, entre_rios, formosa]");
        ubicacion.put("municipios", "Lista de municipios específicos");
        ubicacion.put("usarTodasLasBDS", "Boolean para consultar todas las bases de datos");
        ayuda.put("ubicacion", ubicacion);

        // Sección equipos
        HashMap<String, Object> equipos = new HashMap<>();
        equipos.put("seriesEquipos", "Lista de series de equipos");
        equipos.put("tiposDispositivos", "Lista de tipos: [1=Radar, 2=Cámara, etc.]");
        equipos.put("lugares", "Lista de ubicaciones/lugares");
        ayuda.put("equipos", equipos);

        // Sección infracciones
        HashMap<String, Object> infracciones = new HashMap<>();
        infracciones.put("tiposInfracciones", "Lista de tipos: [1=Velocidad, 3=Luz Roja, 4=Senda Peatonal]");
        infracciones.put("estadosInfracciones", "Lista de estados de infracciones");
        infracciones.put("exportadoSacit", "Boolean para filtrar por exportación a SACIT");
        infracciones.put("montoMinimo/montoMaximo", "Rango de montos");
        ayuda.put("infracciones", infracciones);

        // Formatos (usando Arrays.asList para compatibilidad con Java 8)
        ayuda.put("formatos", Arrays.asList("json", "csv", "excel"));

        // Sección paginación
        HashMap<String, Object> paginacion = new HashMap<>();
        paginacion.put("pagina", "Número de página (empezando en 0)");
        paginacion.put("tamanoPagina", "Registros por página (máximo 1000)");
        paginacion.put("limiteMaximo", "Límite absoluto de registros");
        ayuda.put("paginacion", paginacion);

        return ResponseEntity.ok(ayuda);
    }
}