package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.QueryStorageDTO;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.service.DatabaseQueryService;

import javax.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/queries-db")
@CrossOrigin(origins = "*", maxAge = 3600)
public class DatabaseQueryController {

    @Autowired
    private DatabaseQueryService databaseQueryService;

    // =============== EJECUCIÓN DE QUERIES ===============

    /**
     * ENDPOINT PRINCIPAL: Ejecutar query por código
     * Integra automáticamente con consolidación dinámica
     */
    @PostMapping("/ejecutar/{codigo}")
    public ResponseEntity<?> ejecutarQuery(
            @PathVariable String codigo,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        try {
            log.info("Ejecutando query desde BD: {} - Consolidado: {}",
                    codigo, consulta.getParametrosFiltros() != null &&
                            consulta.getParametrosFiltros().esConsolidado());

            Object resultado = databaseQueryService.ejecutarQueryPorCodigo(codigo, consulta);

            // Headers informativos
            ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok();

            if (consulta.getParametrosFiltros() != null && consulta.getParametrosFiltros().esConsolidado()) {
                responseBuilder
                        .header("X-Query-Consolidada", "true")
                        .header("X-Query-Codigo", codigo);
            }

            return responseBuilder.body(resultado);

        } catch (IllegalArgumentException e) {
            log.error("Query no encontrada o inválida '{}': {}", codigo, e.getMessage());
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Query no válida");
            body.put("codigo", codigo);
            body.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(body);

        } catch (Exception e) {
            log.error("Error ejecutando query '{}': {}", codigo, e.getMessage(), e);
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error interno");
            body.put("codigo", codigo);
            body.put("detalle", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);

        }
    }

    /**
     * Preview de query antes de ejecutar
     */
    @PostMapping("/preview/{codigo}")
    public ResponseEntity<Map<String, Object>> previewQuery(
            @PathVariable String codigo,
            @RequestBody(required = false) ParametrosFiltrosDTO filtros) {

        try {
            Map<String, Object> preview = databaseQueryService.previewQuery(codigo, filtros);
            return ResponseEntity.ok(preview);
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error generando preview");
            body.put("codigo", codigo);
            body.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(body);

        }
    }

    // =============== GESTIÓN CRUD DE QUERIES ===============

    /**
     * Listar todas las queries activas
     */
    @GetMapping
    public ResponseEntity<List<QueryStorage>> listarQueries() {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesActivas();
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error listando queries: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Obtener query específica por código
     */
    @GetMapping("/{codigo}")
    public ResponseEntity<QueryStorage> obtenerQuery(@PathVariable String codigo) {
        try {
            Optional<QueryStorage> query = databaseQueryService.obtenerQueryPorCodigo(codigo);

            if (query.isPresent()) {
                return ResponseEntity.ok(query.get());
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Error obteniendo query '{}': {}", codigo, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Crear nueva query
     */
    @PostMapping
    public ResponseEntity<?> crearQuery(@Valid @RequestBody QueryStorageDTO dto) {
        try {
            QueryStorage queryCreada = databaseQueryService.guardarQuery(dto);

            Map<String, Object> body = new HashMap<>();
            body.put("mensaje", "Query creada exitosamente");
            body.put("codigo", queryCreada.getCodigo());
            body.put("id", queryCreada.getId());
            body.put("consolidable", queryCreada.getEsConsolidable());
            return ResponseEntity.ok(body);

        } catch (IllegalArgumentException e) {

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error de validación");
            body.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(body);

        } catch (Exception e) {
            log.error("Error creando query: {}", e.getMessage(), e);
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error interno");
            body.put("detalle", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);

        }
    }

    /**
     * Actualizar query existente
     */
    @PutMapping("/{codigo}")
    public ResponseEntity<?> actualizarQuery(
            @PathVariable String codigo,
            @Valid @RequestBody QueryStorageDTO dto) {

        try {
            QueryStorage queryActualizada = databaseQueryService.actualizarQuery(codigo, dto);

            Map<String, Object> body = new HashMap<>();
            body.put("mensaje", "Query actualizada exitosamente");
            body.put("codigo", queryActualizada.getCodigo());
            body.put("version", queryActualizada.getVersion());
            body.put("consolidable", queryActualizada.getEsConsolidable());
            return ResponseEntity.ok(body);

        } catch (IllegalArgumentException e) {

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Query no encontrada o inválida");
            body.put("codigo", codigo);
            body.put("detalle", e.getMessage());
            return ResponseEntity.badRequest().body(body);

        } catch (Exception e) {
            log.error("Error actualizando query '{}': {}", codigo, e.getMessage(), e);
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error interno");
            body.put("detalle", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);

        }
    }

    /**
     * Eliminar query (soft delete)
     */
    @DeleteMapping("/{codigo}")
    public ResponseEntity<Map<String, Object>> eliminarQuery(@PathVariable String codigo) {
        try {
            databaseQueryService.eliminarQuery(codigo);

            Map<String, Object> body = new HashMap<>();
            body.put("mensaje", "Query eliminada exitosamente");
            body.put("codigo", codigo);

            return ResponseEntity.ok(body);


        } catch (IllegalArgumentException e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Query no encontrada");
            body.put("codigo", codigo);

            return ResponseEntity.badRequest().body(body);

        } catch (Exception e) {
            log.error("Error eliminando query '{}': {}", codigo, e.getMessage(), e);

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error interno");
            body.put("detalle", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);

        }
    }

    // =============== CARGA DE QUERIES DESDE ARCHIVO ===============

    /**
     * Cargar query desde archivo
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
                Map<String, Object> body = new HashMap<>();
                body.put("error", "Archivo vacío");

                return ResponseEntity.badRequest().body(body);
            }

            // Leer contenido
            String sqlContent = new String(archivo.getBytes(), StandardCharsets.UTF_8);

            // Validar SQL
            if (!databaseQueryService.validarSqlQuery(sqlContent)) {
                Map<String, Object> body = new HashMap<>();
                body.put("error", "SQL no válido o inseguro");
                body.put("detalle", "El archivo debe contener una query SELECT válida con parámetros dinámicos");

                return ResponseEntity.badRequest().body(body);
            }


            // Crear DTO
            QueryStorageDTO dto = QueryStorageDTO.builder()
                    .codigo(codigo)
                    .nombre(nombre)
                    .descripcion(descripcion != null ? descripcion : "Cargada desde archivo: " + archivo.getOriginalFilename())
                    .sqlQuery(sqlContent)
                    .categoria(categoria)
                    .creadoPor(creadoPor != null ? creadoPor : "UPLOAD_USUARIO")
                    .activa(true)
                    .build();

            // Guardar
            QueryStorage queryGuardada = databaseQueryService.guardarQuery(dto);

            Map<String, Object> response = new HashMap<>();
            response.put("mensaje", "Query cargada exitosamente");
            response.put("codigo", queryGuardada.getCodigo());
            response.put("consolidable", queryGuardada.getEsConsolidable());
            response.put("archivo_original", archivo.getOriginalFilename());

            if (queryGuardada.getEsConsolidable()) {
                response.put("campos_agrupacion", queryGuardada.getCamposAgrupacionList());
                response.put("campos_numericos", queryGuardada.getCamposNumericosList());
            }

            return ResponseEntity.ok(response);

        }catch (IllegalArgumentException e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error de validación");
            body.put("detalle", e.getMessage());

            return ResponseEntity.badRequest().body(body);
        }
        catch (Exception e) {
            log.error("Error cargando query desde archivo: {}", e.getMessage(), e);

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error interno");
            body.put("detalle", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
        }

    }

    // =============== BÚSQUEDA Y FILTRADO ===============

    @GetMapping("/buscar")
    public ResponseEntity<List<QueryStorage>> buscarQueries(@RequestParam(required = false) String q) {
        try {
            List<QueryStorage> queries = databaseQueryService.buscarQueries(q);
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error buscando queries: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/categoria/{categoria}")
    public ResponseEntity<List<QueryStorage>> obtenerPorCategoria(@PathVariable String categoria) {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesPorCategoria(categoria);
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error obteniendo queries por categoría '{}': {}", categoria, e.getMessage(), e);
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
    public ResponseEntity<List<QueryStorage>> obtenerQueriesPopulares(@RequestParam(defaultValue = "10") int limite) {
        try {
            List<QueryStorage> queries = databaseQueryService.obtenerQueriesPopulares(limite);
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error obteniendo queries populares: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== HERRAMIENTAS ADMINISTRATIVAS ===============

    /**
     * Migrar queries desde archivos a BD
     */
    @PostMapping("/migrar-archivos")
    public ResponseEntity<Map<String, Object>> migrarDesdeArchivos() {
        try {
            log.info("Iniciando migración de queries desde archivos");

            int migradas = databaseQueryService.migrarQueriesDesdeArchivos();

            Map<String, Object> body = new HashMap<>();
            body.put("mensaje", "Migración completada");
            body.put("queries_migradas", migradas);

            return ResponseEntity.ok(body);


        }catch (Exception e) {
            log.error("Error en migración: {}", e.getMessage(), e);

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error en migración");
            body.put("detalle", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
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
                // Analizar consolidación potencial
                org.transito_seguro.component.QueryAnalyzer analyzer =
                        new org.transito_seguro.component.QueryAnalyzer();
                var analisis = analyzer.analizarParaConsolidacion(sql);

                validacion.put("consolidable", analisis.isEsConsolidado());
                if (analisis.isEsConsolidado()) {
                    validacion.put("campos_agrupacion", analisis.getCamposAgrupacion());
                    validacion.put("campos_numericos", analisis.getCamposNumericos());
                }
            }

            return ResponseEntity.ok(validacion);

        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("sql_valido", false);
            body.put("error", e.getMessage());

            return ResponseEntity.ok(body);
        }

    }

    /**
     * Ejecutar query específicamente en modo consolidado
     */
    @PostMapping("/consolidada/{codigo}")
    public ResponseEntity<?> ejecutarQueryConsolidada(
            @PathVariable String codigo,
            @Valid @RequestBody ConsultaQueryDTO consulta) {

        try {
            // Forzar consolidación
            ParametrosFiltrosDTO filtrosConsolidados = consulta.getParametrosFiltros() != null ?
                    consulta.getParametrosFiltros().toBuilder()
                            .consolidado(true)
                            .build() :
                    ParametrosFiltrosDTO.builder()
                            .consolidado(true)
                            .build();

            consulta.setParametrosFiltros(filtrosConsolidados);

            return ejecutarQuery(codigo, consulta);

        } catch (Exception e) {
            log.error("Error ejecutando query consolidada '{}': {}", codigo, e.getMessage(), e);

            Map<String, Object> body = new HashMap<>();
            body.put("error", "Error en consolidación");
            body.put("codigo", codigo);
            body.put("detalle", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
        }

    }

    // =============== ESTADÍSTICAS Y ANÁLISIS ===============

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

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            Map<String, Object> health = new HashMap<>();

            List<QueryStorage> queries = databaseQueryService.obtenerQueriesActivas();
            int consolidables = (int) queries.stream().mapToLong(q -> q.getEsConsolidable() ? 1 : 0).sum();

            health.put("status", "UP");
            health.put("total_queries_activas", queries.size());
            health.put("queries_consolidables", consolidables);
            health.put("sistema_operativo", queries.size() > 0);

            return ResponseEntity.ok(health);

        } catch (Exception e) {
            log.error("Error en health check: {}", e.getMessage(), e);

            Map<String, Object> body = new HashMap<>();
            body.put("status", "DOWN");
            body.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
        }

    }
}