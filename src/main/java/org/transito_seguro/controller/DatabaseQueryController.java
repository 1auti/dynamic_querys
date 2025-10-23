package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.transito_seguro.component.analyzer.query.QueryAnalyzer;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.QueryStorageDTO;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.service.DatabaseQueryService;

import javax.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Controller avanzado para gestión de queries dinámicas
 * Maneja queries almacenadas en base de datos con análisis automático
 */
@Slf4j
@RestController
@RequestMapping("/api/queries-db")
@CrossOrigin(origins = "*", maxAge = 3600)
public class DatabaseQueryController {

    @Autowired
    private DatabaseQueryService databaseQueryService;

    @Autowired
    private QueryAnalyzer queryAnalyzer;

    // =============== EJECUCIÓN DE QUERIES ===============

    /**
     * Ejecutar query por código con soporte completo de consolidación
     */
    @PostMapping("/ejecutar/{codigo}")
    public ResponseEntity<?> ejecutarQuery(
            @PathVariable String codigo,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        try {
            boolean consolidado = esConsolidado(consulta);

            log.info("Ejecutando query BD: {} - Consolidado: {}", codigo, consolidado);

            Object resultado = databaseQueryService.ejecutarQueryPorCodigo(codigo, consulta);

            // Headers informativos
            ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok();
            responseBuilder.header("X-Query-Fuente", "database");
            responseBuilder.header("X-Query-Codigo", codigo);

            if (consolidado) {
                responseBuilder.header("X-Query-Consolidada", "true");
            }

            return responseBuilder.body(resultado);

        } catch (IllegalArgumentException e) {
            return crearRespuestaError("Query no válida", codigo, e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("Error ejecutando query '{}': {}", codigo, e.getMessage(), e);
            return crearRespuestaError("Error interno", codigo, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Ejecutar query específicamente en modo consolidado
     */
    @PostMapping("/consolidada/{codigo}")
    public ResponseEntity<?> ejecutarQueryConsolidada(
            @PathVariable String codigo,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        // Forzar consolidación
        ParametrosFiltrosDTO filtrosConsolidados = consulta.getParametrosFiltros() != null ?
                consulta.getParametrosFiltros().toBuilder()
                        .consolidado(true)
                        .consolidacion(Arrays.asList("provincia", "municipio"))
                        .build() :
                ParametrosFiltrosDTO.builder()
                        .consolidado(true)
                        .consolidacion(Arrays.asList("provincia", "municipio"))
                        .build();
        consulta.setParametrosFiltros(filtrosConsolidados);

        return ejecutarQuery(codigo, consulta);
    }

    /**
     * Preview de query sin ejecutar
     */
    @PostMapping("/preview/{codigo}")
    public ResponseEntity<Map<String, Object>> previewQuery(
            @PathVariable String codigo,
            @RequestBody(required = false) ParametrosFiltrosDTO filtros) {

        try {
            Map<String, Object> preview = databaseQueryService.previewQuery(codigo, filtros);
            return ResponseEntity.ok(preview);
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(crearMapaError("Error en preview", codigo, e.getMessage()));
        }
    }

    // =============== GESTIÓN DE QUERIES ===============

    /**
     * Listar queries con filtros opcionales
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listarQueries(
            @RequestParam(required = false) String categoria,
            @RequestParam(required = false) Boolean consolidable
           ) {

        try {
            List<QueryStorage> queries;

            if (categoria != null) {
                queries = databaseQueryService.obtenerQueriesPorCategoria(categoria);
            } else if (consolidable != null && consolidable) {
                queries = databaseQueryService.obtenerQueriesConsolidables();
            } else {
                queries = databaseQueryService.obtenerQueriesActivas();
            }

            // Respuesta enriquecida
            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("queries", queries);
            respuesta.put("total", queries.size());
            respuesta.put("consolidables", queries.stream()
                    .mapToLong(q -> q.getEsConsolidable() ? 1 : 0)
                    .sum());

            return ResponseEntity.ok(respuesta);

        } catch (Exception e) {
            log.error("Error listando queries: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Obtener query específica
     */
    @GetMapping("/{codigo}")
    public ResponseEntity<Map<String, Object>> obtenerQuery(@PathVariable String codigo) {
        try {
            Optional<QueryStorage> queryOpt = databaseQueryService.obtenerQueryPorCodigo(codigo);

            if (queryOpt.isPresent()) {
                QueryStorage query = queryOpt.get();

                Map<String, Object> respuesta = new HashMap<>();
                respuesta.put("query", query);
                respuesta.put("estadisticas", databaseQueryService.obtenerEstadisticasUso(codigo));

                return ResponseEntity.ok(respuesta);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Error obteniendo query '{}': {}", codigo, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Crear nueva query con análisis automático
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> crearQuery(@Valid @RequestBody QueryStorageDTO dto) {
        try {
            QueryStorage queryCreada = databaseQueryService.guardarQuery(dto);

            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("mensaje", "Query creada exitosamente");
            respuesta.put("query", queryCreada);
            respuesta.put("consolidable", queryCreada.getEsConsolidable());

            if (queryCreada.getEsConsolidable()) {
                respuesta.put("campos_agrupacion", queryCreada.getCamposAgrupacionList());
                respuesta.put("campos_numericos", queryCreada.getCamposNumericosList());
            }

            return ResponseEntity.ok(respuesta);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(crearMapaError("Error de validación", dto.getCodigo(), e.getMessage()));
        } catch (Exception e) {
            log.error("Error creando query: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(crearMapaError("Error interno", dto.getCodigo(), e.getMessage()));
        }
    }

    /**
     * Actualizar query existente
     */
    @PutMapping("/{codigo}")
    public ResponseEntity<Map<String, Object>> actualizarQuery(
            @PathVariable String codigo,
            @Valid @RequestBody QueryStorageDTO dto) {

        try {
            QueryStorage queryActualizada = databaseQueryService.actualizarQuery(codigo, dto);

            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("mensaje", "Query actualizada exitosamente");
            respuesta.put("query", queryActualizada);
            respuesta.put("version", queryActualizada.getVersion());

            return ResponseEntity.ok(respuesta);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(crearMapaError("Query no encontrada", codigo, e.getMessage()));
        } catch (Exception e) {
            log.error("Error actualizando query '{}': {}", codigo, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(crearMapaError("Error interno", codigo, e.getMessage()));
        }
    }

    /**
     * Eliminar query (soft delete)
     */
    @DeleteMapping("/{codigo}")
    public ResponseEntity<Map<String, Object>> eliminarQuery(@PathVariable String codigo) {
        try {
            databaseQueryService.eliminarQuery(codigo);

            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("mensaje", "Query eliminada exitosamente");
            respuesta.put("codigo", codigo);

            return ResponseEntity.ok(respuesta);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(crearMapaError("Query no encontrada", codigo, e.getMessage()));
        } catch (Exception e) {
            log.error("Error eliminando query '{}': {}", codigo, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(crearMapaError("Error interno", codigo, e.getMessage()));
        }
    }

    // =============== CARGA Y VALIDACIÓN ===============

    /**
     * Cargar query desde archivo con análisis automático
     */
    @PostMapping("/cargar")
    public ResponseEntity<Map<String, Object>> cargarQueryDesdeArchivo(
            @RequestParam("archivo") MultipartFile archivo,
            @RequestParam("codigo") String codigo,
            @RequestParam("nombre") String nombre,
            @RequestParam(value = "descripcion", required = false) String descripcion,
            @RequestParam(value = "categoria", required = false, defaultValue = "GENERAL") String categoria,
            @RequestParam(value = "creadoPor", required = false) String creadoPor) {

        try {
            // Validaciones básicas
            if (archivo.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(crearMapaError("Archivo vacío", codigo, "No se recibió contenido"));
            }

            // Leer contenido
            String sqlContent = new String(archivo.getBytes(), StandardCharsets.UTF_8);

            // Validar SQL
            if (!databaseQueryService.validarSqlQuery(sqlContent)) {
                return ResponseEntity.badRequest()
                        .body(crearMapaError("SQL no válido", codigo,
                                "El archivo debe contener una query SELECT válida"));
            }

            // Crear DTO
            QueryStorageDTO dto = QueryStorageDTO.builder()
                    .codigo(codigo)
                    .nombre(nombre)
                    .descripcion(descripcion != null ? descripcion :
                            "Cargada desde archivo: " + archivo.getOriginalFilename())
                    .sqlQuery(sqlContent)
                    .categoria(categoria)
                    .creadoPor(creadoPor != null ? creadoPor : "UPLOAD_USUARIO")
                    .activa(true)
                    .build();

            // Guardar
            QueryStorage queryGuardada = databaseQueryService.guardarQuery(dto);

            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("mensaje", "Query cargada exitosamente");
            respuesta.put("query", queryGuardada);
            respuesta.put("archivo_original", archivo.getOriginalFilename());

            return ResponseEntity.ok(respuesta);

        } catch (Exception e) {
            log.error("Error cargando query desde archivo: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(crearMapaError("Error interno", codigo, e.getMessage()));
        }
    }

    /**
     * Validar SQL sin guardar
     */
    @PostMapping("/validar-sql")
    public ResponseEntity<Map<String, Object>> validarSQL(@RequestBody String sql) {
        try {
            boolean valido = databaseQueryService.validarSqlQuery(sql);

            Map<String, Object> validacion = new HashMap<>();
            validacion.put("sql_valido", valido);
            validacion.put("longitud", sql.length());
            validacion.put("lineas", sql.split("\n").length);

            if (valido) {
                // Analizar consolidación
                AnalisisConsolidacion analisis =
                        queryAnalyzer.analizarParaConsolidacion(sql);

                validacion.put("consolidable", analisis.esSeguraParaMemoria());
                validacion.put("campos_agrupacion", analisis.getCamposAgrupacion());
                validacion.put("campos_numericos", analisis.getCamposNumericos());
                validacion.put("campos_ubicacion", analisis.getCamposUbicacion());
            }

            return ResponseEntity.ok(validacion);

        } catch (Exception e) {
            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("sql_valido", false);
            respuesta.put("error", e.getMessage());
            return ResponseEntity.ok(respuesta);
        }
    }

    // =============== BÚSQUEDA Y FILTRADO ===============

    @GetMapping("/buscar")
    public ResponseEntity<List<QueryStorage>> buscarQueries(
            @RequestParam(required = false) String q) {
        try {
            List<QueryStorage> queries = databaseQueryService.buscarQueries(q);
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error buscando queries: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/consolidables")
    public ResponseEntity<List<QueryStorage>> obtenerQueriesConsolidables() {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesConsolidables();
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error obteniendo queries consolidables: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/populares")
    public ResponseEntity<List<QueryStorage>> obtenerQueriesPopulares(
            @RequestParam(defaultValue = "10") int limite) {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesPopulares(limite);
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error obteniendo queries populares: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== ESTADÍSTICAS Y ADMINISTRACIÓN ===============

    @GetMapping("/estadisticas")
    public ResponseEntity<Map<String, Object>> obtenerEstadisticas() {
        try {
            Map<String, Object> stats = databaseQueryService.obtenerEstadisticas();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error obteniendo estadísticas: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{codigo}/estadisticas")
    public ResponseEntity<Map<String, Object>> obtenerEstadisticasQuery(@PathVariable String codigo) {
        try {
            Map<String, Object> stats = databaseQueryService.obtenerEstadisticasUso(codigo);
            return ResponseEntity.ok(stats);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            log.error("Error obteniendo estadísticas de query '{}': {}", codigo, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/migrar-archivos")
    public ResponseEntity<Map<String, Object>> migrarDesdeArchivos() {
        try {
            log.info("Iniciando migración de queries desde archivos");
            int migradas = databaseQueryService.migrarQueriesDesdeArchivos();

            Map<String, Object> respuesta = new HashMap<>();
            respuesta.put("mensaje", "Migración completada");
            respuesta.put("queries_migradas", migradas);

            return ResponseEntity.ok(respuesta);

        } catch (Exception e) {
            log.error("Error en migración: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(crearMapaError("Error en migración", "migración", e.getMessage()));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesActivas();
            int consolidables = (int) queries.stream()
                    .mapToLong(q -> q.getEsConsolidable() ? 1 : 0)
                    .sum();

            Map<String, Object> health = new HashMap<>();
            health.put("status", "UP");
            health.put("total_queries", queries.size());
            health.put("queries_consolidables", consolidables);
            health.put("porcentaje_consolidables",
                    queries.size() > 0 ? (double) consolidables / queries.size() * 100 : 0);

            return ResponseEntity.ok(health);

        } catch (Exception e) {
            log.error("Error en health check: {}", e.getMessage(), e);

            Map<String, Object> errorHealth = new HashMap<>();
            errorHealth.put("status", "DOWN");
            errorHealth.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorHealth);
        }
    }

    // =============== MÉTODOS UTILITARIOS CORREGIDOS ===============

    private boolean esConsolidado(ConsultaQueryDTO consulta) {
        return consulta.getParametrosFiltros() != null &&
                consulta.getParametrosFiltros().esConsolidado();
    }

    /**
     * CORREGIDO: Crear respuesta de error que devuelve ResponseEntity
     */
    private ResponseEntity<Map<String, Object>> crearRespuestaError(String error, String codigo,
                                                                    String detalle, HttpStatus status) {
        Map<String, Object> respuesta = new HashMap<>();
        respuesta.put("error", error);
        respuesta.put("codigo", codigo);
        respuesta.put("detalle", detalle);
        respuesta.put("timestamp", java.time.LocalDateTime.now());
        respuesta.put("status", status.value());

        return ResponseEntity.status(status).body(respuesta);
    }

    /**
     * Crear mapa de error para métodos que ya retornan ResponseEntity
     */
    private Map<String, Object> crearMapaError(String error, String codigo, String detalle) {
        Map<String, Object> mapa = new HashMap<>();
        mapa.put("error", error);
        mapa.put("codigo", codigo);
        mapa.put("detalle", detalle);
        mapa.put("timestamp", java.time.LocalDateTime.now());
        return mapa;
    }
}