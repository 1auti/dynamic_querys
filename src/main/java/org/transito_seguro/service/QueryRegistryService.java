package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryStorage;
import org.transito_seguro.repository.QueryStorageRepository;
import org.transito_seguro.utils.SqlUtils;

import java.util.List;
import java.util.Optional;

/**
 * 🔍 Servicio Registry que gestiona análisis automático de queries
 * y proporciona metadata para consolidación
 */
@Slf4j
@Service
@Transactional
public class QueryRegistryService {

    @Autowired
    private QueryStorageRepository queryRepository;

    @Autowired
    private QueryAnalyzer queryAnalyzer;

    /**
     * 🎯 MÉTODO PRINCIPAL: Obtener análisis de consolidación
     * Busca primero en BD, si no existe analiza dinámicamente
     */
    public QueryAnalyzer.AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        log.debug("Obteniendo análisis de consolidación para: {}", nombreQuery);

        // 1. Buscar en BD primero
        String codigoQuery = convertirNombreACodigo(nombreQuery);
        Optional<QueryStorage> queryStorage = queryRepository.findByCodigo(codigoQuery);

        if (queryStorage.isPresent() && queryStorage.get().estaLista()) {
            QueryStorage query = queryStorage.get();
            log.debug("Análisis encontrado en BD para query: {}", codigoQuery);

            return crearAnalisisDesdeQueryStorage(query);
        }

        // 2. Fallback: Análisis dinámico desde archivo
        try {
            String sql = SqlUtils.cargarQuery(nombreQuery);
            QueryAnalyzer.AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(sql);

            log.debug("Análisis dinámico completado para: {}", nombreQuery);
            return analisis;

        } catch (Exception e) {
            log.warn("Error en análisis dinámico para '{}': {}", nombreQuery, e.getMessage());
            return crearAnalisisVacio();
        }
    }

    /**
     * 🔄 Registrar/actualizar query desde archivo automáticamente
     */
    @EventListener(ApplicationReadyEvent.class)
    public void inicializarRegistryAlInicio() {
        log.info("🚀 Iniciando auto-registro de queries al arrancar la aplicación");

        int registradas = autoRegistrarQueriesDesdeArchivos();
        log.info("✅ Auto-registro completado: {} queries procesadas", registradas);
    }

    /**
     * 📁 Auto-registra todas las queries desde archivos
     */
    public int autoRegistrarQueriesDesdeArchivos() {
        int procesadas = 0;

        // Iterar sobre todas las queries del enum
        for (org.transito_seguro.enums.Consultas consulta :
                org.transito_seguro.enums.Consultas.values()) {

            try {
                String codigo = convertirConsultaACodigo(consulta);

                // Solo procesar si no existe en BD
                if (!queryRepository.findByCodigo(codigo).isPresent()) {
                    registrarQueryDesdeArchivo(consulta);
                    procesadas++;
                } else {
                    log.debug("Query '{}' ya existe en BD, saltando", codigo);
                }

            } catch (Exception e) {
                log.error("Error procesando query '{}': {}",
                        consulta.getArchivoQuery(), e.getMessage());
            }
        }

        return procesadas;
    }

    /**
     * 💾 Registrar una query específica desde archivo
     */
    public QueryStorage registrarQueryDesdeArchivo(org.transito_seguro.enums.Consultas consulta) {
        String codigo = convertirConsultaACodigo(consulta);

        try {
            // Cargar SQL del archivo
            String sql = SqlUtils.cargarQuery(consulta.getArchivoQuery());

            // Analizar automáticamente
            QueryAnalyzer.AnalisisConsolidacion analisis =
                    queryAnalyzer.analizarParaConsolidacion(sql);

            // Crear QueryStorage
            QueryStorage queryStorage = QueryStorage.builder()
                    .codigo(codigo)
                    .nombre(consulta.getDescripcion())
                    .descripcion("Auto-registrada desde: " + consulta.getArchivoQuery())
                    .sqlQuery(sql)
                    .categoria(determinarCategoriaPorNombre(codigo))
                    .esConsolidable(analisis.isEsConsolidado())
                    .activa(true)
                    .estado(EstadoQuery.ANALIZADA)
                    .creadoPor("AUTO_REGISTRY")
                    .build();

            // Aplicar análisis si es consolidable
            if (analisis.isEsConsolidado()) {
                queryStorage.setCamposAgrupacionList(analisis.getCamposAgrupacion());
                queryStorage.setCamposNumericosList(analisis.getCamposNumericos());
                queryStorage.setCamposUbicacionList(analisis.getCamposUbicacion());
                queryStorage.setCamposTiempoList(analisis.getCamposTiempo());
            }

            QueryStorage guardada = queryRepository.save(queryStorage);

            log.info("✅ Query auto-registrada: {} -> {} (Consolidable: {})",
                    consulta.getArchivoQuery(), codigo, guardada.getEsConsolidable());

            return guardada;

        } catch (Exception e) {
            log.error("❌ Error registrando query '{}': {}",
                    consulta.getArchivoQuery(), e.getMessage(), e);
            throw new RuntimeException("Error en auto-registro", e);
        }
    }

    /**
     * 🔍 Buscar query por múltiples criterios
     */
    public Optional<QueryStorage> buscarQuery(String identificador) {
        // 1. Buscar por código exacto
        Optional<QueryStorage> resultado = queryRepository.findByCodigo(identificador);
        if (resultado.isPresent()) {
            return resultado;
        }

        // 2. Convertir nombre de archivo a código y buscar
        String codigoPosible = convertirNombreACodigo(identificador);
        return queryRepository.findByCodigo(codigoPosible);
    }

    /**
     * ♻️ Re-analizar query existente
     */
    public QueryStorage reAnalizarQuery(String codigo) {
        QueryStorage query = queryRepository.findByCodigo(codigo)
                .orElseThrow(() -> new IllegalArgumentException("Query no encontrada: " + codigo));

        try {
            // Re-analizar SQL actual
            QueryAnalyzer.AnalisisConsolidacion analisis =
                    queryAnalyzer.analizarParaConsolidacion(query.getSqlQuery());

            // Actualizar metadata
            query.setEsConsolidable(analisis.isEsConsolidado());
            query.setEstado(EstadoQuery.ANALIZADA);

            if (analisis.isEsConsolidado()) {
                query.setCamposAgrupacionList(analisis.getCamposAgrupacion());
                query.setCamposNumericosList(analisis.getCamposNumericos());
                query.setCamposUbicacionList(analisis.getCamposUbicacion());
                query.setCamposTiempoList(analisis.getCamposTiempo());
            } else {
                // Limpiar metadata si ya no es consolidable
                query.setCamposAgrupacionList(null);
                query.setCamposNumericosList(null);
                query.setCamposUbicacionList(null);
                query.setCamposTiempoList(null);
            }

            QueryStorage actualizada = queryRepository.save(query);

            log.info("🔄 Query re-analizada: {} (Consolidable: {})",
                    codigo, actualizada.getEsConsolidable());

            return actualizada;

        } catch (Exception e) {
            query.setEstado(EstadoQuery.ERROR);
            queryRepository.save(query);

            log.error("❌ Error re-analizando query '{}': {}", codigo, e.getMessage());
            throw new RuntimeException("Error en re-análisis", e);
        }
    }

    // =============== MÉTODOS UTILITARIOS ===============

    /**
     * Crea análisis desde QueryStorage
     */
    private QueryAnalyzer.AnalisisConsolidacion crearAnalisisDesdeQueryStorage(QueryStorage query) {
        return new QueryAnalyzer.AnalisisConsolidacion(
                query.getCamposAgrupacionList(),
                query.getCamposNumericosList(),
                query.getCamposTiempoList(),
                query.getCamposUbicacionList(),
                java.util.Collections.emptyMap(), // tipoPorCampo - no lo almacenamos actualmente
                query.getEsConsolidable()
        );
    }

    /**
     * Convierte nombre de archivo a código de BD
     */
    private String convertirNombreACodigo(String nombreArchivo) {
        return nombreArchivo
                .replace(".sql", "")
                .replace("_", "-")
                .toLowerCase();
    }

    /**
     * Convierte Consulta enum a código
     */
    private String convertirConsultaACodigo(org.transito_seguro.enums.Consultas consulta) {
        return consulta.name().toLowerCase().replace("_", "-");
    }

    /**
     * Determina categoría por nombre
     */
    private String determinarCategoriaPorNombre(String codigo) {
        if (codigo.contains("persona")) return "PERSONAS";
        if (codigo.contains("vehiculo")) return "VEHICULOS";
        if (codigo.contains("infraccion")) return "INFRACCIONES";
        if (codigo.contains("radar")) return "RADARES";
        if (codigo.contains("semaforo")) return "SEMAFOROS";
        if (codigo.contains("reporte")) return "REPORTES";
        if (codigo.contains("verificar")) return "VERIFICACION";
        return "GENERAL";
    }

    /**
     * Análisis vacío para casos de error
     */
    private QueryAnalyzer.AnalisisConsolidacion crearAnalisisVacio() {
        return new QueryAnalyzer.AnalisisConsolidacion(
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(),
                false
        );
    }

    // =============== MÉTODOS PÚBLICOS ADICIONALES ===============

    /**
     * 📊 Obtener estadísticas del registry
     */
    public java.util.Map<String, Object> obtenerEstadisticasRegistry() {
        java.util.Map<String, Object> stats = new java.util.HashMap<>();

        List<QueryStorage> todasActivas = queryRepository.findByActivaTrueOrderByNombreAsc();
        List<QueryStorage> consolidables = queryRepository.findByEsConsolidableAndActivaTrueOrderByNombreAsc(true);

        stats.put("total_queries_registry", todasActivas.size());
        stats.put("queries_consolidables_registry", consolidables.size());
        stats.put("porcentaje_consolidables",
                todasActivas.size() > 0 ?
                        (double) consolidables.size() / todasActivas.size() * 100 : 0);

        // Estadísticas por estado
        java.util.Map<String, Long> porEstado = new java.util.HashMap<>();
        for (EstadoQuery estado : EstadoQuery.values()) {
            long count = queryRepository.findByEstadoAndActivaTrueOrderByNombreAsc(estado).size();
            porEstado.put(estado.name(), count);
        }
        stats.put("queries_por_estado", porEstado);

        return stats;
    }

    /**
     * 🧹 Limpieza y mantenimiento
     */
    public int limpiarQueriesInactivas() {
        List<QueryStorage> inactivas = queryRepository.findAll().stream()
                .filter(q -> !q.getActiva() || q.getEstado() == EstadoQuery.OBSOLETA)
                .filter(q -> q.getContadorUsos() == 0)
                .collect(java.util.stream.Collectors.toList());

        queryRepository.deleteAll(inactivas);

        log.info("🧹 Limpieza completada: {} queries inactivas eliminadas", inactivas.size());
        return inactivas.size();
    }

    /**
     * 🔄 Actualizar todas las queries desde archivos
     */
    public int actualizarTodasDesdeArchivos() {
        int actualizadas = 0;

        for (org.transito_seguro.enums.Consultas consulta :
                org.transito_seguro.enums.Consultas.values()) {

            try {
                String codigo = convertirConsultaACodigo(consulta);
                Optional<QueryStorage> existente = queryRepository.findByCodigo(codigo);

                if (existente.isPresent()) {
                    // Actualizar SQL desde archivo
                    String nuevoSql = SqlUtils.cargarQuery(consulta.getArchivoQuery());
                    QueryStorage query = existente.get();

                    if (!query.getSqlQuery().equals(nuevoSql)) {
                        query.setSqlQuery(nuevoSql);
                        query.setVersion(query.getVersion() + 1);

                        // Re-analizar
                        reAnalizarQuery(codigo);
                        actualizadas++;

                        log.info("🔄 Query actualizada desde archivo: {}", codigo);
                    }
                }

            } catch (Exception e) {
                log.error("Error actualizando query desde archivo '{}': {}",
                        consulta.getArchivoQuery(), e.getMessage());
            }
        }

        log.info("✅ Actualización completada: {} queries actualizadas", actualizadas);
        return actualizadas;
    }
}