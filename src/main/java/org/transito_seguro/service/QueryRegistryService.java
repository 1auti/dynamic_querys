package org.transito_seguro.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryMetadata;
import org.transito_seguro.repository.QueryMetadataRepository;
import org.transito_seguro.utils.SqlUtils;

import javax.annotation.PostConstruct;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@Transactional
public class QueryRegistryService {

    @Autowired
    private QueryMetadataRepository repository;

    @Autowired
    private QueryAnalyzer queryAnalyzer;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${app.query-analyzer.version:1.0}")
    private String versionAnalisis;

    @Value("${app.query-registry.auto-analyze-on-startup:true}")
    private boolean autoAnalyzeOnStartup;

    private static final String HASH_ALGORITHM = "SHA-256";

    @PostConstruct
    public void init() {
        log.info("QueryRegistryService inicializado:");
        log.info("  - Versión análisis: {}", versionAnalisis);
        log.info("  - Auto-análisis: {}", autoAnalyzeOnStartup);

        if (autoAnalyzeOnStartup) {
            analizarQuerysExistentes();
        }
    }

    // =============== MÉTODOS PRINCIPALES ===============

    /**
     * 🚀 MÉTODO PRINCIPAL: Obtiene metadata de consolidación para una query
     * Si no está registrada, la analiza automáticamente
     */
    public QueryAnalyzer.AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        log.debug("Obteniendo análisis para query: {}", nombreQuery);

        try {
            // 1. Buscar en base de datos
            Optional<QueryMetadata> metadataOpt = repository.findByNombreQuery(nombreQuery);

            QueryMetadata metadata;
            if (metadataOpt.isPresent()) {
                metadata = metadataOpt.get();


                // Verificar si necesita re-análisis
                String hashActual = calcularHashQuery(nombreQuery);
                if (metadata.necesitaReanalisis(hashActual, versionAnalisis)) {
                    log.info("Query '{}' necesita re-análisis", nombreQuery);
                    metadata = reAnalizarQuery(metadata, hashActual);
                }

                repository.save(metadata);

            } else {
                // Query nueva - analizar automáticamente
                log.info("Query '{}' no registrada, analizando automáticamente", nombreQuery);
                metadata = analizarYRegistrarQuery(nombreQuery);
            }

            // Convertir a AnalisisConsolidacion
            return convertirAAnalisisConsolidacion(metadata);

        } catch (Exception e) {
            log.error("Error obteniendo análisis para query '{}': {}", nombreQuery, e.getMessage(), e);
            return crearAnalisisFallback();
        }
    }

    /**
     * 🔄 Re-analiza una query existente
     */
    private QueryMetadata reAnalizarQuery(QueryMetadata metadata, String hashActual) {
        try {
            String querySQL = SqlUtils.cargarQuery(metadata.getNombreQuery());
            QueryAnalyzer.AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(querySQL);

            return actualizarMetadataConAnalisis(metadata, analisis, hashActual);

        } catch (Exception e) {
            log.error("Error re-analizando query '{}': {}", metadata.getNombreQuery(), e.getMessage());
            metadata.setEstado(EstadoQuery.ERROR);
            metadata.setErrorUltimoAnalisis(e.getMessage());
            return metadata;
        }
    }

    /**
     * ✨ Analiza y registra una query nueva automáticamente
     */
    private QueryMetadata analizarYRegistrarQuery(String nombreQuery) {
        try {
            String querySQL = SqlUtils.cargarQuery(nombreQuery);
            String hashQuery = calcularHashQuery(querySQL);

            // Realizar análisis
            QueryAnalyzer.AnalisisConsolidacion analisis = queryAnalyzer.analizarParaConsolidacion(querySQL);

            // Crear metadata
            QueryMetadata metadata = QueryMetadata.builder()
                    .nombreQuery(nombreQuery)
                    .hashQuery(hashQuery)
                    .esRegistrada(false) // Auto-registrada
                    .versionAnalisis(versionAnalisis)
                    .estado(EstadoQuery.ANALIZADA)
                    .categoria(determinarCategoria(nombreQuery))
                    .build();

            // Aplicar análisis
            actualizarMetadataConAnalisis(metadata, analisis, hashQuery);

            // Guardar en BD
            metadata = repository.save(metadata);
            log.info("Query '{}' registrada automáticamente - Consolidable: {}",
                    nombreQuery, metadata.getEsConsolidable());

            return metadata;

        } catch (Exception e) {
            log.error("Error auto-registrando query '{}': {}", nombreQuery, e.getMessage(), e);

            // Crear registro con error para evitar re-intentos constantes
            QueryMetadata errorMetadata = QueryMetadata.builder()
                    .nombreQuery(nombreQuery)
                    .hashQuery("ERROR")
                    .esRegistrada(false)
                    .esConsolidable(false)
                    .estado(EstadoQuery.ERROR)
                    .errorUltimoAnalisis(e.getMessage())
                    .versionAnalisis(versionAnalisis)
                    .build();

            return repository.save(errorMetadata);
        }
    }

    public List<QueryMetadata> traerTodasLasQuerys(){
        return repository.findAll();
    }

    /**
     * 📊 Actualiza metadata con resultados del análisis
     */
    private QueryMetadata actualizarMetadataConAnalisis(QueryMetadata metadata,
                                                        QueryAnalyzer.AnalisisConsolidacion analisis,
                                                        String hashActual) {
        metadata.setHashQuery(hashActual);
        metadata.setEsConsolidable(analisis.isEsConsolidado());
        metadata.setCamposAgrupacionList(analisis.getCamposAgrupacion());
        metadata.setCamposNumericosList(analisis.getCamposNumericos());
        metadata.setCamposUbicacionList(analisis.getCamposUbicacion());
        metadata.setCamposTiempoList(analisis.getCamposTiempo());
        metadata.setRequiereReanalisis(false);
        metadata.setErrorUltimoAnalisis(null);
        metadata.setEstado(EstadoQuery.ANALIZADA);
        metadata.setVersionAnalisis(versionAnalisis);

        return metadata;
    }

    // =============== REGISTRO MANUAL ===============

    /**
     * 👨‍💼 Registro manual por administrador
     */
    public QueryMetadata registrarQueryManualmente(String nombreQuery,
                                                   List<String> camposAgrupacion,
                                                   List<String> camposNumericos,
                                                   String descripcion) {
        log.info("Registrando query manualmente: {}", nombreQuery);

        try {
            String querySQL = SqlUtils.cargarQuery(nombreQuery);
            String hashQuery = calcularHashQuery(querySQL);

            Optional<QueryMetadata> existente = repository.findByNombreQuery(nombreQuery);
            QueryMetadata metadata;

            if (existente.isPresent()) {
                metadata = existente.get();
                log.info("Actualizando query existente: {}", nombreQuery);
            } else {
                metadata = QueryMetadata.builder()
                        .nombreQuery(nombreQuery)
                        .categoria(determinarCategoria(nombreQuery))
                        .build();
            }

            // Configuración manual
            metadata.setHashQuery(hashQuery);
            metadata.setCamposAgrupacionList(camposAgrupacion);
            metadata.setCamposNumericosList(camposNumericos);
            metadata.setEsConsolidable(!camposNumericos.isEmpty());
            metadata.marcarComoRegistrada(descripcion);
            metadata.setVersionAnalisis(versionAnalisis);

            return repository.save(metadata);

        } catch (Exception e) {
            log.error("Error en registro manual de query '{}': {}", nombreQuery, e.getMessage(), e);
            throw new RuntimeException("Error registrando query manualmente", e);
        }
    }

    // =============== ANÁLISIS BATCH ===============

    /**
     * 🔄 Analiza todas las queries existentes en el directorio
     */
    public void analizarQuerysExistentes() {
        log.info("Iniciando análisis automático de queries existentes");

        try {
            // Obtener todas las queries del enum
            org.transito_seguro.enums.Consultas[] consultas =
                    org.transito_seguro.enums.Consultas.values();

            int procesadas = 0;
            int errores = 0;

            for (org.transito_seguro.enums.Consultas consulta : consultas) {
                try {
                    String nombreQuery = consulta.getArchivoQuery();
                    obtenerAnalisisConsolidacion(nombreQuery); // Esto auto-registra si no existe
                    procesadas++;
                } catch (Exception e) {
                    log.error("Error analizando query '{}': {}", consulta.getArchivoQuery(), e.getMessage());
                    errores++;
                }
            }

            log.info("Análisis completado: {} procesadas, {} errores", procesadas, errores);

        } catch (Exception e) {
            log.error("Error en análisis batch de queries: {}", e.getMessage(), e);
        }
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private String calcularHashQuery(String queryContent) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = digest.digest(queryContent.getBytes());
            StringBuilder hexString = new StringBuilder();

            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (Exception e) {
            log.error("Error calculando hash: {}", e.getMessage());
            return "ERROR_" + System.currentTimeMillis();
        }
    }

    private String determinarCategoria(String nombreQuery) {
        String nombre = nombreQuery.toLowerCase();

        if (nombre.contains("persona")) return "PERSONAS";
        if (nombre.contains("vehiculo")) return "VEHICULOS";
        if (nombre.contains("infraccion")) return "INFRACCIONES";
        if (nombre.contains("radar")) return "RADARES";
        if (nombre.contains("semaforo")) return "SEMAFOROS";
        if (nombre.contains("reporte")) return "REPORTES";

        return "GENERAL";
    }

    private String determinarComplejidad(String querySQL) {
        int lineas = querySQL.split("\n").length;
        int subqueries = (querySQL.split("SELECT").length - 1);

        if (lineas > 100 || subqueries > 3) return "COMPLEJA";
        if (lineas > 50 || subqueries > 1) return "MEDIA";
        return "SIMPLE";
    }

    private QueryAnalyzer.AnalisisConsolidacion convertirAAnalisisConsolidacion(QueryMetadata metadata) {
        if (!metadata.estaListaParaConsolidacion()) {
            return crearAnalisisFallback();
        }

        return new QueryAnalyzer.AnalisisConsolidacion(
                metadata.getCamposAgrupacionList(),
                metadata.getCamposNumericosList(),
                metadata.getCamposTiempoList(),
                metadata.getCamposUbicacionList(),
                java.util.Collections.emptyMap(), // tipoPorCampo - no necesario para consolidación
                metadata.getEsConsolidable()
        );
    }

    private QueryAnalyzer.AnalisisConsolidacion crearAnalisisFallback() {
        return new QueryAnalyzer.AnalisisConsolidacion(
                java.util.Arrays.asList("provincia"),
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList(),
                java.util.Arrays.asList("provincia"),
                java.util.Collections.emptyMap(),
                false
        );
    }

    // =============== MÉTODOS PÚBLICOS DE CONSULTA ===============

    public List<QueryMetadata> obtenerQuerysPendientesAnalisis() {
        return repository.findByRequiereReanalisis(true);
    }

    public List<QueryMetadata> obtenerQueriesConsolidables() {
        return repository.findByEsConsolidable(true);
    }

    public Optional<QueryMetadata> obtenerMetadataQuery(String nombreQuery) {
        return repository.findByNombreQuery(nombreQuery);
    }
}
