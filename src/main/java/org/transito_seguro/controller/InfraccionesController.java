package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.*;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.service.InfraccionesService;
import org.transito_seguro.component.ConsultaValidator;

import javax.validation.Valid;
import javax.xml.bind.ValidationException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Controller para endpoints tradicionales de infracciones.
 *
 * MEJORAS v2.0:
 * - Validaciones exhaustivas de entrada
 * - Feedback detallado de errores
 * - Manejo específico de excepciones SQL
 * - Respuestas enriquecidas con metadatos
 * - Logging mejorado para debugging
 *
 * @author Sistema Tránsito Seguro
 * @version 2.0 - Con validaciones mejoradas
 */
@Slf4j
@RestController
@RequestMapping("/api/infracciones")
@CrossOrigin(origins = "*", maxAge = 3600)
public class InfraccionesController {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private InfraccionesService infraccionesService;

    @Autowired
    private ConsultaValidator consultaValidator;

    @Value("${app.limits.max-records-display:100000}")
    private int maxRecordsDisplay;

    @Value("${app.limits.max-records-download:1000000}")
    private int maxRecordsDownload;

    // =============== ENDPOINTS PRINCIPALES ===============

    /**
     * Endpoint principal genérico - maneja todas las consultas.
     *
     * VALIDACIONES:
     * - Tipo de consulta existe
     * - Parámetros válidos (fechas, provincias, formato)
     * - Límites razonables
     * - Consolidación configurada correctamente
     *
     * @param tipoConsulta Código de la query a ejecutar
     * @param consulta DTO con parámetros de filtros
     * @param bindingResult Resultado de validación de Spring
     * @return Resultados de la consulta o errores detallados
     */
    @PostMapping("/{tipoConsulta}")
    public ResponseEntity<?> ejecutarConsulta(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta,
            BindingResult bindingResult) {

        long tiempoInicio = System.currentTimeMillis();

        try {
            // VALIDACIÓN 1: Errores de binding de Spring (anotaciones @Valid)
            if (bindingResult.hasErrors()) {
                return manejarErroresValidacionSpring(bindingResult, tipoConsulta);
            }

            // VALIDACIÓN 2: Tipo de consulta no puede estar vacío
            if (tipoConsulta == null || tipoConsulta.trim().isEmpty()) {
                return crearRespuestaError(
                        "Tipo de consulta requerido",
                        "Debe especificar el código de la consulta en la URL",
                        HttpStatus.BAD_REQUEST,
                        null
                );
            }

            // VALIDACIÓN 3: Consulta debe tener estructura básica
            if (consulta == null) {
                return crearRespuestaError(
                        "Body requerido",
                        "La petición debe incluir un body JSON con la estructura de consulta",
                        HttpStatus.BAD_REQUEST,
                        ejemploConsultaBasica()
                );
            }

            // VALIDACIÓN 4: Inicializar filtros si no existen
            if (consulta.getParametrosFiltros() == null) {
                consulta.setParametrosFiltros(new ParametrosFiltrosDTO());
            }

            // VALIDACIÓN 5: Validar estructura completa de la consulta
            List<String> erroresValidacion = validarConsultaCompleta(consulta);
            if (!erroresValidacion.isEmpty()) {
                return crearRespuestaErrorMultiple(
                        "Errores de validación",
                        erroresValidacion,
                        HttpStatus.BAD_REQUEST
                );
            }

            // VALIDACIÓN 6: Aplicar límites de seguridad
            aplicarLimitesSeguridad(consulta);

            // PROCESAMIENTO: Ejecutar la consulta
            log.info("🔍 Ejecutando consulta: {} - Consolidado: {} - Límite: {}",
                    tipoConsulta,
                    esConsolidado(consulta),
                    consulta.getParametrosFiltros().getLimiteEfectivo());

            Object resultado = infraccionesService.ejecutarConsultaPorTipo(tipoConsulta, consulta);

            // RESPUESTA EXITOSA con metadatos
            long tiempoTotal = System.currentTimeMillis() - tiempoInicio;
            return construirRespuestaExitosa(resultado, consulta, tipoConsulta, tiempoTotal);

        } catch (ValidationException e) {
            // Error de validación del servicio
            log.warn("❌ Validación fallida en {}: {}", tipoConsulta, e.getMessage());
            return crearRespuestaError(
                    "Error de validación",
                    e.getMessage(),
                    HttpStatus.BAD_REQUEST,
                    obtenerSugerenciasValidacion(e.getMessage())
            );

        } catch (IllegalArgumentException e) {
            // Query no existe o parámetros incorrectos
            log.error("❌ Argumento inválido en {}: {}", tipoConsulta, e.getMessage());
            return crearRespuestaError(
                    "Consulta no soportada",
                    e.getMessage(),
                    HttpStatus.BAD_REQUEST,
                    obtenerSugerenciasQuery(tipoConsulta)
            );

        } catch (Exception e) {
            log.error("❌ Error en {}: {}", tipoConsulta, e.getMessage(), e);

            // Detectar si es SQL
            if (esSQLException(e)) {
                return manejarErrorSQL(e, tipoConsulta);
            }

            // Otros errores
            Map<String, String> sugerenciasError = new HashMap<>();
            sugerenciasError.put("soporte", "Contacte al administrador...");
            sugerenciasError.put("tipo_error", e.getClass().getSimpleName());

            return crearRespuestaError(
                    "Error interno del servidor",
                    "Ocurrió un error inesperado procesando la consulta",
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    sugerenciasError
            );
        }
    }

    /**
     * Endpoint de descarga de archivos.
     * Elimina límites para obtener todos los datos.
     */
    @PostMapping("/{tipoConsulta}/descargar")
    public ResponseEntity<byte[]> descargarArchivo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta,
            BindingResult bindingResult) {

        try {
            // VALIDACIÓN 1: Errores de Spring
            if (bindingResult.hasErrors()) {
                log.error("Errores de validación en descarga: {}",
                        bindingResult.getAllErrors().stream()
                                .map(e -> e.getDefaultMessage())
                                .collect(Collectors.joining(", ")));
                return ResponseEntity.badRequest().build();
            }

            // VALIDACIÓN 2: Formato debe estar especificado para descarga
            if (consulta.getFormato() == null || consulta.getFormato().trim().isEmpty()) {
                log.error("Formato no especificado para descarga");
                return ResponseEntity.badRequest()
                        .header("X-Error", "Debe especificar el formato (csv, excel, json)")
                        .build();
            }

            // VALIDACIÓN 3: Formato debe ser válido
            String formato = consulta.getFormato().toLowerCase();
            if (!Arrays.asList("csv", "excel", "json").contains(formato)) {
                log.error("Formato inválido para descarga: {}", formato);
                return ResponseEntity.badRequest()
                        .header("X-Error", "Formato debe ser csv, excel o json")
                        .build();
            }

            log.info("📥 Descarga - Tipo: {}, Formato: {}, Consolidado: {}",
                    tipoConsulta, formato, esConsolidado(consulta));

            // Configurar para descarga SIN límite
            if (consulta.getParametrosFiltros() == null) {
                consulta.setParametrosFiltros(new ParametrosFiltrosDTO());
            }

            // Establecer sin límite para obtener TODOS los datos
            consulta.getParametrosFiltros().setLimite(null);
            consulta.getParametrosFiltros().setUsarTodasLasBDS(true);

            log.info("✅ Descarga configurada SIN LÍMITE para obtener todos los datos");

            return infraccionesService.descargarConsultaPorTipo(tipoConsulta, consulta);

        } catch (IllegalArgumentException e) {
            log.error("❌ Tipo no válido para descarga: {}", e.getMessage());
            return ResponseEntity.badRequest()
                    .header("X-Error", e.getMessage())
                    .build();

        } catch (Exception e) {
            log.error("❌ Error en descarga {}: {}", tipoConsulta, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .header("X-Error", "Error interno: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Endpoint alternativo de descarga.
     */
    @PostMapping("/descargar/{tipoConsulta}")
    public ResponseEntity<byte[]> descargarArchivoAlternativo(
            @PathVariable String tipoConsulta,
            @Valid @RequestBody ConsultaQueryDTO consulta,
            BindingResult bindingResult) {
        return descargarArchivo(tipoConsulta, consulta, bindingResult);
    }

    // =============== MÉTODOS DE VALIDACIÓN ===============

    /**
     * Valida la estructura completa de la consulta.
     * Retorna lista de errores encontrados.
     */
    private List<String> validarConsultaCompleta(ConsultaQueryDTO consulta) {
        List<String> errores = new ArrayList<>();
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Validar fechas
        if (filtros.getFechaInicio() != null && filtros.getFechaFin() != null) {
            if (filtros.getFechaInicio().after(filtros.getFechaFin())) {
                errores.add("La fecha de inicio debe ser anterior a la fecha fin");
            }
        }

        // No permitir fecha específica Y rango al mismo tiempo
        if (filtros.getFechaEspecifica() != null &&
                (filtros.getFechaInicio() != null || filtros.getFechaFin() != null)) {
            errores.add("No puede usar fecha específica y rango de fechas simultáneamente");
        }

        // Validar formato si está presente
        if (consulta.getFormato() != null && !consulta.getFormato().trim().isEmpty()) {
            String formato = consulta.getFormato().toLowerCase();
            if (!Arrays.asList("csv", "excel", "json").contains(formato)) {
                errores.add("Formato '" + consulta.getFormato() + "' no válido. Use: csv, excel o json");
            }
        }

        // Validar límites si están especificados
        if (filtros.getLimite() != null) {
            if (filtros.getLimite() < 1) {
                errores.add("El límite debe ser mayor a 0");
            }
            if (filtros.getLimite() > maxRecordsDownload) {
                errores.add("El límite excede el máximo permitido: " + maxRecordsDownload);
            }
        }

        // Validar paginación
        if (filtros.getPagina() != null && filtros.getPagina() < 1) {
            errores.add("El número de página debe ser mayor a 0");
        }

        if (filtros.getTamanoPagina() != null && filtros.getTamanoPagina() < 1) {
            errores.add("El tamaño de página debe ser mayor a 0");
        }

        // Validar consolidación
        if (filtros.getConsolidado() != null && filtros.getConsolidado()) {
            if (filtros.getConsolidacion() == null || filtros.getConsolidacion().isEmpty()) {
                errores.add("Para consultas consolidadas debe especificar campos de agrupación");
            }
        }

        // Validar provincias/bases de datos
        if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            Set<String> provinciasPermitidas = consultaValidator.getProvinciasPermitidas();
            Set<String> codigosPermitidos = consultaValidator.getCodigosPermitidos();

            for (String bd : filtros.getBaseDatos()) {
                if (!provinciasPermitidas.contains(bd) && !codigosPermitidos.contains(bd)) {
                    errores.add("Provincia/código '" + bd + "' no válido. " +
                            "Disponibles: " + String.join(", ", provinciasPermitidas));
                }
            }
        }

        return errores;
    }

    /**
     * Aplica límites de seguridad para prevenir consultas muy grandes.
     */
    private void aplicarLimitesSeguridad(ConsultaQueryDTO consulta) {
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Si no hay límite explícito y no es consolidado, aplicar límite default
        if (filtros.getLimite() == null &&
                !filtros.esConsolidado() &&
                (filtros.getUsarTodasLasBDS() == null || !filtros.getUsarTodasLasBDS())) {

            filtros.setLimite(maxRecordsDisplay);
            log.info("🔒 Límite de seguridad aplicado: {}", maxRecordsDisplay);
        }

        // Advertir si el límite es muy alto
        Integer limiteEfectivo = filtros.getLimiteEfectivo();
        if (limiteEfectivo != null && limiteEfectivo > maxRecordsDisplay) {
            log.warn("⚠️ Límite alto solicitado: {} (recomendado: {})",
                    limiteEfectivo, maxRecordsDisplay);
        }
    }

    /**
     * Maneja errores de validación de Spring (anotaciones @Valid).
     */
    private ResponseEntity<?> manejarErroresValidacionSpring(
            BindingResult bindingResult,
            String tipoConsulta) {

        List<String> errores = bindingResult.getAllErrors().stream()
                .map(error -> {
                    if (error instanceof FieldError) {
                        FieldError fieldError = (FieldError) error;
                        return fieldError.getField() + ": " + fieldError.getDefaultMessage();
                    }
                    return error.getDefaultMessage();
                })
                .collect(Collectors.toList());

        log.warn("❌ Errores de validación en {}: {}", tipoConsulta, errores);

        return crearRespuestaErrorMultiple(
                "Errores de validación en los datos de entrada",
                errores,
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Maneja específicamente errores de SQL con información útil.
     * Ahora acepta Exception genérica para mayor flexibilidad.
     */
    private ResponseEntity<?> manejarErrorSQL(Exception e, String tipoConsulta) {
        String mensajeUsuario;
        String detalleTecnico = e.getMessage() != null ? e.getMessage() : "Error SQL sin mensaje";
        Map<String, Object> sugerencias = new HashMap<>();

        // Clasificar el tipo de error SQL
        if (detalleTecnico.toLowerCase().contains("syntax")) {
            mensajeUsuario = "Error de sintaxis en la consulta SQL";
            sugerencias.put("causa", "La query tiene un error de sintaxis SQL");
            sugerencias.put("accion", "Revise la definición de la query en la base de datos");

        } else if (detalleTecnico.toLowerCase().contains("column") &&
                detalleTecnico.toLowerCase().contains("not found")) {
            mensajeUsuario = "Columna no encontrada en la base de datos";
            sugerencias.put("causa", "La query referencia una columna que no existe");
            sugerencias.put("accion", "Verifique los nombres de columnas en la query");

        } else if (detalleTecnico.toLowerCase().contains("table") &&
                detalleTecnico.toLowerCase().contains("not found")) {
            mensajeUsuario = "Tabla no encontrada en la base de datos";
            sugerencias.put("causa", "La query referencia una tabla que no existe");
            sugerencias.put("accion", "Verifique el nombre de la tabla y los permisos");

        } else if (detalleTecnico.toLowerCase().contains("timeout") ||
                detalleTecnico.toLowerCase().contains("connection")) {
            mensajeUsuario = "Problema de conexión con la base de datos";
            sugerencias.put("causa", "Timeout o error de conexión");
            sugerencias.put("accion", "Intente nuevamente en unos momentos");

        } else if (detalleTecnico.toLowerCase().contains("permission") ||
                detalleTecnico.toLowerCase().contains("access denied")) {
            mensajeUsuario = "Permisos insuficientes para ejecutar la consulta";
            sugerencias.put("causa", "El usuario de BD no tiene permisos suficientes");
            sugerencias.put("accion", "Contacte al administrador");

        } else {
            mensajeUsuario = "Error de base de datos";
            sugerencias.put("causa", "Error SQL no categorizado");
            sugerencias.put("detalle", detalleTecnico);
        }

        return crearRespuestaError(
                mensajeUsuario,
                detalleTecnico,
                HttpStatus.INTERNAL_SERVER_ERROR,
                sugerencias
        );
    }

    /**
     * Verifica si una excepción es de tipo SQL (por clase o causa).
     */
    private boolean esSQLException(Exception e) {
        // Verificar si es SQLException directamente
        if (e.getClass().getName().contains("SQLException")) {
            return true;
        }

        // Verificar la causa
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause.getClass().getName().contains("SQLException")) {
                return true;
            }
            cause = cause.getCause();
        }

        // Verificar por mensaje común de SQL
        String mensaje = e.getMessage();
        if (mensaje != null) {
            String mensajeLower = mensaje.toLowerCase();
            return mensajeLower.contains("sql") ||
                    mensajeLower.contains("jdbc") ||
                    mensajeLower.contains("database") ||
                    mensajeLower.contains("syntax error") ||
                    mensajeLower.contains("column") ||
                    mensajeLower.contains("table");
        }

        return false;
    }

    // =============== MÉTODOS DE CONSTRUCCIÓN DE RESPUESTAS ===============

    /**
     * Construye una respuesta exitosa con metadatos enriquecidos.
     */
    private ResponseEntity<?> construirRespuestaExitosa(
            Object resultado,
            ConsultaQueryDTO consulta,
            String tipoConsulta,
            long tiempoEjecucion) {

        ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok();
        boolean consolidado = esConsolidado(consulta);

        // Agregar headers informativos
        responseBuilder
                .header("X-Tipo-Consulta", tipoConsulta)
                .header("X-Tiempo-Ejecucion-Ms", String.valueOf(tiempoEjecucion))
                .header("X-Consolidado", String.valueOf(consolidado));

        if (consolidado) {
            responseBuilder.header("X-Campos-Agrupacion",
                    String.join(",", consulta.getParametrosFiltros().getConsolidacion()));
        }

        // Si el resultado es una lista, agregar el tamaño
        if (resultado instanceof List) {
            responseBuilder.header("X-Total-Registros", String.valueOf(((List<?>) resultado).size()));
        } else if (resultado instanceof Map) {
            Map<?, ?> mapa = (Map<?, ?>) resultado;
            if (mapa.containsKey("datos") && mapa.get("datos") instanceof List) {
                responseBuilder.header("X-Total-Registros",
                        String.valueOf(((List<?>) mapa.get("datos")).size()));
            }
        }

        log.info("✅ Consulta exitosa: {} - Tiempo: {}ms - Consolidado: {}",
                tipoConsulta, tiempoEjecucion, consolidado);

        return responseBuilder.body(resultado);
    }

    /**
     * Crea una respuesta de error estándar con información detallada.
     */
    private ResponseEntity<?> crearRespuestaError(
            String error,
            String detalle,
            HttpStatus status,
            Object sugerencias) {

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("error", error);
        errorResponse.put("detalle", detalle);
        errorResponse.put("status", status.value());
        errorResponse.put("timestamp", new Date());

        if (sugerencias != null) {
            errorResponse.put("sugerencias", sugerencias);
        }

        return ResponseEntity.status(status).body(errorResponse);
    }

    /**
     * Crea una respuesta de error con múltiples errores de validación.
     */
    private ResponseEntity<?> crearRespuestaErrorMultiple(
            String mensaje,
            List<String> errores,
            HttpStatus status) {

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("error", mensaje);
        errorResponse.put("errores", errores);
        errorResponse.put("total_errores", errores.size());
        errorResponse.put("status", status.value());
        errorResponse.put("timestamp", new Date());

        return ResponseEntity.status(status).body(errorResponse);
    }

    // =============== MÉTODOS DE SUGERENCIAS ===============

    /**
     * Obtiene sugerencias basadas en el mensaje de validación.
     */
    private Map<String, String> obtenerSugerenciasValidacion(String mensajeError) {
        Map<String, String> sugerencias = new HashMap<>();

        String mensajeLower = mensajeError.toLowerCase();

        if (mensajeLower.contains("fecha")) {
            sugerencias.put("fechas", "Use formato ISO: yyyy-MM-dd'T'HH:mm:ss");
            sugerencias.put("ejemplo", "2024-01-15T00:00:00");
        }

        if (mensajeLower.contains("formato")) {
            sugerencias.put("formatos_validos", "csv, excel, json");
        }

        if (mensajeLower.contains("provincia") || mensajeLower.contains("base de datos")) {
            sugerencias.put("provincias", "Use nombres completos o códigos como 'pba', 'mda', etc.");
        }

        if (mensajeLower.contains("consolidación") || mensajeLower.contains("consolidado")) {
            sugerencias.put("consolidacion", "Debe especificar campos de agrupación");
            sugerencias.put("ejemplo", "[\"provincia\", \"municipio\"]");
        }

        return sugerencias;
    }

    /**
     * Obtiene sugerencias cuando una query no existe o no es válida.
     */
    private Map<String, String> obtenerSugerenciasQuery(String tipoConsulta) {
        Map<String, String> sugerencias = new HashMap<>();

        sugerencias.put("query_solicitada", tipoConsulta);
        sugerencias.put("accion", "Use el endpoint GET /api/queries para ver las queries disponibles");
        sugerencias.put("alternativa", "Verifique el código de la query en la documentación");

        return sugerencias;
    }

    /**
     * Retorna un ejemplo de consulta básica para ayudar al usuario.
     */
    private Map<String, Object> ejemploConsultaBasica() {
        Map<String, Object> ejemplo = new LinkedHashMap<>();

        ejemplo.put("formato", "json");

        Map<String, Object> parametros = new LinkedHashMap<>();
        parametros.put("limite", 100);
        parametros.put("fechaInicio", "2024-01-01T00:00:00");
        parametros.put("fechaFin", "2024-12-31T23:59:59");
        parametros.put("baseDatos", Arrays.asList("pba", "mda"));

        ejemplo.put("parametrosFiltros", parametros);

        return ejemplo;
    }

    // =============== MÉTODOS UTILITARIOS ===============

    /**
     * Verifica si la consulta es consolidada.
     */
    private boolean esConsolidado(ConsultaQueryDTO consulta) {
        return consulta.getParametrosFiltros() != null &&
                consulta.getParametrosFiltros().esConsolidado();
    }
}