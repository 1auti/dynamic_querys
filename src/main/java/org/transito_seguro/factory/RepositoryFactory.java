package org.transito_seguro.factory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.transito_seguro.config.ProvinciaMapping;
import org.transito_seguro.repository.InfraccionesRepository;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.Map;

@Component
public class RepositoryFactory {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ProvinciaMapping provinciaMapping;

    public InfraccionesRepository getRepository(String provincia) {
        // Obtener el nombre del datasource desde el mapeo
        String datasourceName = provinciaMapping.getDatasourceForProvincia(provincia);

        if (datasourceName == null) {
            throw new IllegalArgumentException("Provincia no soportada: " + provincia);
        }

        // Construir el nombre del bean JdbcTemplate
        String jdbcTemplateBeanName = datasourceName + "JdbcTemplate";

        // Obtener el JdbcTemplate específico desde el contexto de Spring
        NamedParameterJdbcTemplate jdbcTemplate = applicationContext.getBean(
                jdbcTemplateBeanName,
                NamedParameterJdbcTemplate.class
        );

        // Crear y devolver el repository con el JdbcTemplate específico
        return new InfraccionesRepositoryImpl(jdbcTemplate, provincia);
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
     * Verificar si una provincia está soportada
     */
    public boolean isProvinciaSupported(String provincia) {
        return provinciaMapping.isProvinciaSupported(provincia);
    }

    /**
     * Obtener todas las provincias soportadas
     */
    public java.util.Set<String> getProvinciasSoportadas() {
        return provinciaMapping.getMapping().keySet();
    }
}