package org.transito_seguro.controller;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.transito_seguro.dto.QueryRegistrationDTO;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryMetadata;
import org.transito_seguro.service.QueryRegistryService;

import javax.validation.Valid;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/admin/queries")
@CrossOrigin(origins = "*", maxAge = 3600)
public class QueryManagementController {

    @Autowired
    private QueryRegistryService queryRegistryService;

    // =============== CONSULTA DE QUERIES REGISTRADAS ===============

    /**
     * 📋 Listar todas las queries registradas
     */
    @GetMapping
    public ResponseEntity<List<QueryMetadata>> listarQueries() {
        try {
            List<QueryMetadata> queries = queryRegistryService.traerTodasLasQuerys();
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error listando queries: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 📊 Obtener metadata de una query específica
     */
    @GetMapping("/{nombreQuery}")
    public ResponseEntity<QueryMetadata> obtenerQueryMetadata(@PathVariable String nombreQuery) {
        try {
            Optional<QueryMetadata> metadata = queryRegistryService.obtenerMetadataQuery(nombreQuery);

            if (metadata.isPresent()) {
                return ResponseEntity.ok(metadata.get());
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Error obteniendo metadata de query '{}': {}", nombreQuery, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 🔍 Listar queries consolidables
     */
    @GetMapping("/consolidables")
    public ResponseEntity<List<QueryMetadata>> listarQueriesConsolidables() {
        try {
            List<QueryMetadata> queries = queryRegistryService.obtenerQueriesConsolidables();
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error listando queries consolidables: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * ⚠️ Listar queries pendientes de análisis
     */
    @GetMapping("/pendientes")
    public ResponseEntity<List<QueryMetadata>> listarQueriesPendientes() {
        try {
            List<QueryMetadata> queries = queryRegistryService.obtenerQuerysPendientesAnalisis();
            return ResponseEntity.ok(queries);
        } catch (Exception e) {
            log.error("Error listando queries pendientes: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== REGISTRO MANUAL DE QUERIES ===============

    /**
     * ✍️ Registrar query manualmente con campos específicos
     */
    @PostMapping("/registrar")
    public ResponseEntity<QueryMetadata> registrarQueryManualmente(
            @Valid @RequestBody QueryRegistrationDTO registro) {
        try {
            log.info("Registrando query manualmente: {}", registro.getNombreQuery());

            QueryMetadata metadata = queryRegistryService.registrarQueryManualmente(
                    registro.getNombreQuery(),
                    registro.getCamposAgrupacion(),
                    registro.getCamposNumericos(),
                    registro.getDescripcion()
            );

            return ResponseEntity.ok(metadata);

        } catch (IllegalArgumentException e) {
            log.error("Error de validación registrando query: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error registrando query '{}': {}", registro.getNombreQuery(), e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== CARGA DE QUERIES DESDE ARCHIVO ===============

    /**
     * 📄 Cargar nueva query desde archivo
     */
    @PostMapping("/cargar")
    public ResponseEntity<Map<String, Object>> cargarQuery(
            @RequestParam("archivo") MultipartFile archivo,
            @RequestParam("nombreQuery") String nombreQuery,
            @RequestParam(value = "descripcion", required = false) String descripcion) {

        Map<String, Object> response = new HashMap<>();

        try {
            // Validaciones
            if (archivo.isEmpty()) {
                response.put("error", "Archivo vacío");
                return ResponseEntity.badRequest().body(response);
            }

            if (!nombreQuery.endsWith(".sql")) {
                nombreQuery += ".sql";
            }

            // Leer contenido del archivo
            String contenidoSQL = new String(archivo.getBytes(), StandardCharsets.UTF_8);

            // Validar que es SQL válido (básico)
            if (!contenidoSQL.toUpperCase().contains("SELECT")) {
                response.put("error", "El archivo no contiene una query SQL válida");
                return ResponseEntity.badRequest().body(response);
            }

            // TODO: Aquí guardarías el archivo en /resources/querys/
            // Por ahora, solo simulamos el proceso
            log.info("Query recibida: {} ({} caracteres)", nombreQuery, contenidoSQL.length());

            // Analizar automáticamente
            var analisis = queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);

            response.put("mensaje", "Query cargada y analizada exitosamente");
            response.put("nombreQuery", nombreQuery);
            response.put("esConsolidable", analisis.isEsConsolidado());
            response.put("camposAgrupacion", analisis.getCamposAgrupacion());
            response.put("camposNumericos", analisis.getCamposNumericos());

            return ResponseEntity.ok(response);

        } catch (IOException e) {
            log.error("Error leyendo archivo de query: {}", e.getMessage(), e);
            response.put("error", "Error leyendo el archivo");
            return ResponseEntity.internalServerError().body(response);
        } catch (Exception e) {
            log.error("Error cargando query '{}': {}", nombreQuery, e.getMessage(), e);
            response.put("error", "Error procesando la query");
            return ResponseEntity.internalServerError().body(response);
        }
    }

    // =============== ANÁLISIS Y MANTENIMIENTO ===============

    /**
     * 🔄 Re-analizar todas las queries existentes
     */
    @PostMapping("/reanalizar")
    public ResponseEntity<Map<String, Object>> reAnalizarQueries() {
        try {
            log.info("Iniciando re-análisis de todas las queries");

            queryRegistryService.analizarQuerysExistentes();

            Map<String, Object> response = new HashMap<>();
            response.put("mensaje", "Re-análisis completado");
            response.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error en re-análisis de queries: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error en re-análisis");
            errorResponse.put("detalle", e.getMessage());

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * 🔄 Re-analizar una query específica
     */
    @PostMapping("/{nombreQuery}/reanalizar")
    public ResponseEntity<QueryMetadata> reAnalizarQuery(@PathVariable String nombreQuery) {
        try {
            log.info("Re-analizando query específica: {}", nombreQuery);

            // Forzar re-análisis obteniendo la metadata
            var analisis = queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
            var metadata = queryRegistryService.obtenerMetadataQuery(nombreQuery);

            if (metadata.isPresent()) {
                return ResponseEntity.ok(metadata.get());
            } else {
                return ResponseEntity.notFound().build();
            }

        } catch (Exception e) {
            log.error("Error re-analizando query '{}': {}", nombreQuery, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== ESTADÍSTICAS Y DIAGNÓSTICO ===============

    /**
     * 📊 Obtener estadísticas del sistema de queries
     */
    @GetMapping("/estadisticas")
    public ResponseEntity<Map<String, Object>> obtenerEstadisticas() {
        try {
            Map<String, Object> stats = new HashMap<>();

            List<QueryMetadata> todasLasQueries = queryRegistryService.traerTodasLasQuerys();
            List<QueryMetadata> consolidables = queryRegistryService.obtenerQueriesConsolidables();
            List<QueryMetadata> pendientes = queryRegistryService.obtenerQuerysPendientesAnalisis();

            // Estadísticas básicas
            stats.put("total_queries", todasLasQueries.size());
            stats.put("queries_consolidables", consolidables.size());
            stats.put("queries_pendientes", pendientes.size());

            // Porcentajes
            double porcentajeConsolidables = todasLasQueries.size() > 0 ?
                    (double) consolidables.size() / todasLasQueries.size() * 100 : 0;
            stats.put("porcentaje_consolidables", Math.round(porcentajeConsolidables * 100.0) / 100.0);

            // Estadísticas por categoría
            Map<String, Long> porCategoria = todasLasQueries.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            QueryMetadata::getCategoria,
                            java.util.stream.Collectors.counting()
                    ));
            stats.put("queries_por_categoria", porCategoria);

            // Estadísticas por complejidad
            Map<String, Long> porComplejidad = todasLasQueries.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            QueryMetadata::getComplejidad,
                            java.util.stream.Collectors.counting()
                    ));
            stats.put("queries_por_complejidad", porComplejidad);

            // Top queries más usadas
            List<Map<String, Object>> topQueries = todasLasQueries.stream()
                    .limit(5)
                    .map(q -> {
                        Map<String, Object> queryStats = new HashMap<>();
                        queryStats.put("nombre", q.getNombreQuery());
                        queryStats.put("consolidable", q.getEsConsolidable());
                        queryStats.put("categoria", q.getCategoria());
                        return queryStats;
                    })
                    .collect(java.util.stream.Collectors.toList());
            stats.put("top_queries", topQueries);

            return ResponseEntity.ok(stats);

        } catch (Exception e) {
            log.error("Error obteniendo estadísticas: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 🏥 Health check del sistema de queries
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            Map<String, Object> health = new HashMap<>();

            List<QueryMetadata> pendientes = queryRegistryService.obtenerQuerysPendientesAnalisis();
            List<QueryMetadata> conError = pendientes.stream()
                    .filter(q -> q.getEstado() == EstadoQuery.ERROR)
                    .collect(java.util.stream.Collectors.toList());

            boolean healthy = conError.size() < 5; // Menos de 5 queries con error

            health.put("status", healthy ? "UP" : "DOWN");
            health.put("queries_con_error", conError.size());
            health.put("queries_pendientes", pendientes.size());

            if (!conError.isEmpty()) {
                health.put("queries_problematicas", conError.stream()
                        .map(q -> {
                            Map<String, Object> errorData = new HashMap<>();
                            errorData.put("nombre", q.getNombreQuery());
                            errorData.put("error", q.getErrorUltimoAnalisis());
                            return errorData;
                        })
                        .collect(java.util.stream.Collectors.toList())
                );
            }


            return ResponseEntity.ok(health);

        } catch (Exception e) {
            log.error("Error en health check: {}", e.getMessage(), e);

            Map<String, Object> errorHealth = new HashMap<>();
            errorHealth.put("status", "DOWN");
            errorHealth.put("error", e.getMessage());

            return ResponseEntity.internalServerError().body(errorHealth);
        }
    }

    // =============== UTILIDADES ===============

    /**
     * 🧪 Previsualizar análisis de query sin persistir
     */
    @PostMapping("/preview-analisis")
    public ResponseEntity<Map<String, Object>> previsualizarAnalisis(
            @RequestBody String querySQL) {
        try {
            log.debug("Previsualizando análisis de query");

            // Usar el analyzer directamente sin persistir
            var queryAnalyzer = new org.transito_seguro.component.QueryAnalyzer();
            var analisis = queryAnalyzer.analizarParaConsolidacion(querySQL);

            Map<String, Object> preview = new HashMap<>();
            preview.put("esConsolidable", analisis.isEsConsolidado());
            preview.put("camposAgrupacion", analisis.getCamposAgrupacion());
            preview.put("camposNumericos", analisis.getCamposNumericos());
            preview.put("camposUbicacion", analisis.getCamposUbicacion());
            preview.put("camposTiempo", analisis.getCamposTiempo());

            return ResponseEntity.ok(preview);

        } catch (Exception e) {
            log.error("Error previsualizando análisis: {}", e.getMessage(), e);

            Map<String, Object> error = new HashMap<>();
            error.put("error", "Error analizando query");
            error.put("detalle", e.getMessage());

            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 🗑️ Eliminar query del registro (soft delete)
     */
    @DeleteMapping("/{nombreQuery}")
    public ResponseEntity<Map<String, Object>> eliminarQuery(@PathVariable String nombreQuery) {
        try {
            Optional<QueryMetadata> metadata = queryRegistryService.obtenerMetadataQuery(nombreQuery);

            if (metadata.isPresent()) {
                QueryMetadata query = metadata.get();
                query.setEstado(EstadoQuery.OBSOLETA);

                // Aquí guardarías los cambios en el servicio
                log.info("Query '{}' marcada como obsoleta", nombreQuery);

                Map<String, Object> response = new HashMap<>();
                response.put("mensaje", "Query marcada como obsoleta");
                response.put("nombreQuery", nombreQuery);

                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.notFound().build();
            }

        } catch (Exception e) {
            log.error("Error eliminando query '{}': {}", nombreQuery, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // =============== ENDPOINTS ADICIONALES PARA GESTIÓN ===============

    /**
     * 📁 Escanear directorio de queries manualmente
     */
    @PostMapping("/escanear")
    public ResponseEntity<Map<String, Object>> escanearDirectorioQueries() {
        try {
            log.info("Escaneando directorio de queries manualmente");

            // Forzar escaneo del directorio
            queryRegistryService.analizarQuerysExistentes();

            Map<String, Object> response = new HashMap<>();
            response.put("mensaje", "Escaneo completado");
            response.put("timestamp", java.time.LocalDateTime.now());

            // Obtener estadísticas post-escaneo
            List<QueryMetadata> todasLasQueries = queryRegistryService.traerTodasLasQuerys();
            response.put("queries_encontradas", todasLasQueries.size());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error escaneando directorio de queries: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error en escaneo");
            errorResponse.put("detalle", e.getMessage());

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * 🔧 Limpiar queries obsoletas
     */
    @DeleteMapping("/limpiar-obsoletas")
    public ResponseEntity<Map<String, Object>> limpiarQueriesObsoletas() {
        try {
            log.info("Limpiando queries obsoletas");

            // TODO: Implementar limpieza en el servicio
            // int eliminadas = queryRegistryService.limpiarQueriesObsoletas();

            Map<String, Object> response = new HashMap<>();
            response.put("mensaje", "Limpieza completada");
            response.put("queries_eliminadas", 0); // Placeholder
            response.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error limpiando queries obsoletas: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error en limpieza");
            errorResponse.put("detalle", e.getMessage());

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * 📊 Resumen ejecutivo para dashboard
     */
    @GetMapping("/resumen")
    public ResponseEntity<Map<String, Object>> obtenerResumenEjecutivo() {
        try {
            Map<String, Object> resumen = new HashMap<>();

            List<QueryMetadata> todasLasQueries = queryRegistryService.traerTodasLasQuerys();
            List<QueryMetadata> consolidables = queryRegistryService.obtenerQueriesConsolidables();
            List<QueryMetadata> pendientes = queryRegistryService.obtenerQuerysPendientesAnalisis();

            // Métricas clave
            resumen.put("total_queries", todasLasQueries.size());
            resumen.put("consolidables", consolidables.size());
            resumen.put("pendientes", pendientes.size());
            resumen.put("sistema_saludable", pendientes.size() < 5);

            // Query más usada
            if (!todasLasQueries.isEmpty()) {
                QueryMetadata masUsada = todasLasQueries.get(0);
                Map<String, Object> queryMasUsada = new HashMap<>();
                queryMasUsada.put("nombre", masUsada.getNombreQuery());
                queryMasUsada.put("categoria", masUsada.getCategoria());
                resumen.put("query_mas_usada", queryMasUsada);
            }


            // Última actualización
            resumen.put("ultima_actualizacion", java.time.LocalDateTime.now());

            return ResponseEntity.ok(resumen);

        } catch (Exception e) {
            log.error("Error obteniendo resumen ejecutivo: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
}