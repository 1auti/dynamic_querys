package org.transito_seguro.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.transito_seguro.repository.QueryMetadataRepository;

import javax.annotation.PostConstruct;

@Slf4j
@Service
@Transactional
public class QueryRegisterService {

    @Autowired
    private QueryMetadataRepository repository;

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

    




}
