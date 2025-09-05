package org.transito_seguro.factory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.ParametrosProcessor;
import org.transito_seguro.config.ProvinciaMapping;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.Map;
import java.util.Set;

@Component
public class RepositoryFactory {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ProvinciaMapping provinciaMapping;

    @Autowired
    private ParametrosProcessor parametrosProcessor;

    /**
     * Obtiene un repository por nombre de provincia o código de datasource
     * Acepta tanto "Buenos Aires" como "pba"
     */
    public InfraccionesRepository getRepository(String provinciaOCodigo) {
        String datasourceName = resolverDatasource(provinciaOCodigo);

        if (datasourceName == null) {
            throw new IllegalArgumentException("Provincia/código no soportado: " + provinciaOCodigo);
        }

        // Construir el nombre del bean JdbcTemplate
        String jdbcTemplateBeanName = datasourceName + "JdbcTemplate";

        // Obtener el JdbcTemplate específico desde el contexto de Spring
        NamedParameterJdbcTemplate jdbcTemplate = applicationContext.getBean(
                jdbcTemplateBeanName,
                NamedParameterJdbcTemplate.class
        );

        // Para el repository, usar el nombre original como identificador
        String provinciaIdentificador = obtenerNombreProvincia(provinciaOCodigo);

        return new InfraccionesRepositoryImpl(jdbcTemplate, provinciaIdentificador, parametrosProcessor);
    }

    /**
     * Resuelve el nombre del datasource a partir de un nombre de provincia o código
     */
    private String resolverDatasource(String provinciaOCodigo) {
        Map<String, String> mapping = provinciaMapping.getMapping();
        if (mapping == null) {
            return null;
        }

        // Verificar si es un nombre de provincia
        if (mapping.containsKey(provinciaOCodigo)) {
            return mapping.get(provinciaOCodigo);
        }

        // Verificar si ya es un código de datasource
        if (mapping.containsValue(provinciaOCodigo)) {
            return provinciaOCodigo;
        }

        return null;
    }

    /**
     * Obtiene el nombre de provincia a partir de un código o nombre
     */
    private String obtenerNombreProvincia(String provinciaOCodigo) {
        Map<String, String> mapping = provinciaMapping.getMapping();
        if (mapping == null) {
            return provinciaOCodigo;
        }

        // Si es un nombre de provincia, devolverlo
        if (mapping.containsKey(provinciaOCodigo)) {
            return provinciaOCodigo;
        }

        // Si es un código, buscar el nombre correspondiente
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            if (entry.getValue().equals(provinciaOCodigo)) {
                return entry.getKey();
            }
        }

        return provinciaOCodigo; // Fallback
    }

    /**
     * Obtener todos los repositories para todas las provincias configuradas
     */
    public Map<String, InfraccionesRepository> getAllRepositories() {
        return provinciaMapping.getMapping().keySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        provincia -> provincia,
                        this::getRepository
                ));
    }

    /**
     * Verificar si una provincia está soportada (acepta nombres y códigos)
     */
    public boolean isProvinciaSupported(String provinciaOCodigo) {
        return resolverDatasource(provinciaOCodigo) != null;
    }

    /**
     * Obtener todas las provincias soportadas (nombres completos)
     */
    public Set<String> getProvinciasSoportadas() {
        return provinciaMapping.getMapping() != null ?
                provinciaMapping.getMapping().keySet() :
                java.util.Collections.emptySet();
    }

    /**
     * Obtener todos los códigos de datasource soportados
     */
    public Set<String> getCodigosSoportados() {
        return provinciaMapping.getMapping() != null ?
                new java.util.HashSet<>(provinciaMapping.getMapping().values()) :
                java.util.Collections.emptySet();
    }

    /**
     * Método utilitario para obtener información completa de mapeo
     */
    public Map<String, String> getMapeoCompleto() {
        return provinciaMapping.getMapping() != null ?
                java.util.Collections.unmodifiableMap(provinciaMapping.getMapping()) :
                java.util.Collections.emptyMap();
    }
}