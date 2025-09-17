package org.transito_seguro.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Autowired
    private ObjectMapper objectMapper;

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
    public ResponseEntity<?> infraccionesPorEstado(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("infracciones-por-estado", consulta);
    }

    @PostMapping("/reporte-diego")
    public ResponseEntity<?> reporteDieog(@Valid @RequestBody ConsultaQueryDTO consulta) {
        return ejecutarConsultaConLimite("reporte-diego", consulta);
    }

    // =============== MÉTODOS PRIVADOS MEJORADOS ===============

    /**
     * Método que aplica límite automático y centraliza el manejo de errores
     * DETECTA AUTOMÁTICAMENTE si es consolidación y maneja la respuesta de forma unificada
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

            // Ejecutar consulta
            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consultaConLimite);

            // Log de depuración (opcional - remover en producción)
            logearTipoResultado(resultado, "Resultado del servicio");

            // ✅ UNIFICADO: Extraer datos crudos sin wrappers innecesarios
            Object datosLimpios = extraerDatosCrudos(resultado);

            // Log de depuración (opcional - remover en producción)
            logearTipoResultado(datosLimpios, "Datos después de extracción");

            // ✅ RESPUESTA UNIFICADA con headers informativos opcionales
            ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok();

            if (esConsolidado) {
                responseBuilder
                        .header("X-Consolidado", "true")
                        .header("X-Tipo-Consulta", "consolidada");
            }

            return responseBuilder.body(datosLimpios);

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
     * ✅ MÉTODO UNIFICADO para extraer datos crudos (sin wrappers como {"total": X, "datos": []})
     * Maneja todos los casos: String JSON, Map, List, etc.
     */
    private Object extraerDatosCrudos(Object resultado) {
        try {
            if (resultado instanceof String) {
                String resultadoStr = (String) resultado;

                // Si ya es un array JSON directo, no procesar
                if (resultadoStr.trim().startsWith("[")) {
                    return resultado;
                }

                // Si es un objeto JSON, intentar extraer "datos"
                if (resultadoStr.trim().startsWith("{")) {
                    Map<String, Object> datosParseados = objectMapper.readValue(resultadoStr, Map.class);
                    Object datos = datosParseados.get("datos");

                    // Si tiene "datos", devolverlo; sino devolver todo
                    if (datos != null) {
                        // Convertir de vuelta a JSON string si es necesario
                        return objectMapper.writeValueAsString(datos);
                    }
                }

                return resultado;

            } else if (resultado instanceof Map) {
                Map<?, ?> resultadoMap = (Map<?, ?>) resultado;
                Object datos = resultadoMap.get("datos");

                // Si tiene "datos", devolverlo; sino devolver todo el Map
                return datos != null ? datos : resultado;

            } else if (resultado instanceof List) {
                // Si ya es una lista, devolverla directamente
                return resultado;
            }

            // Para cualquier otro tipo, devolver tal como está
            return resultado;

        } catch (Exception e) {
            log.error("Error extrayendo datos crudos de tipo {}: {}",
                    resultado != null ? resultado.getClass().getSimpleName() : "null",
                    e.getMessage());
            return resultado; // Fallback seguro
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
     * Método de depuración para entender el tipo de datos que se están procesando
     * ⚠️ REMOVER EN PRODUCCIÓN o cambiar log level a TRACE
     */
    private void logearTipoResultado(Object resultado, String contexto) {
        if (log.isDebugEnabled()) {
            if (resultado == null) {
                log.debug("{} - Tipo: null", contexto);
            } else if (resultado instanceof String) {
                String str = (String) resultado;
                String preview = str.length() > 100 ? str.substring(0, 100) + "..." : str;
                log.debug("{} - Tipo: String, Longitud: {}, Preview: '{}'",
                        contexto, str.length(), preview);
            } else if (resultado instanceof List) {
                List<?> list = (List<?>) resultado;
                log.debug("{} - Tipo: List, Tamaño: {}", contexto, list.size());
            } else if (resultado instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) resultado;
                log.debug("{} - Tipo: Map, Keys: {}", contexto, map.keySet());
            } else {
                log.debug("{} - Tipo: {}", contexto, resultado.getClass().getSimpleName());
            }
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