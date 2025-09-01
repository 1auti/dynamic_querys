package org.transito_seguro.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.ConsultaValidator;
import org.transito_seguro.component.FormatoConverter;
import org.transito_seguro.component.ParametrosProcessor;

import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.InfraccionesRepository;


import javax.xml.bind.ValidationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class InfraccionesService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private ConsultaValidator validator;

    @Autowired
    private ParametrosProcessor parametrosProcessor;

    @Autowired
    private FormatoConverter formatoConverter;

    private final Executor executor = Executors.newFixedThreadPool(5);

    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        // 1. Validar consulta
        validator.validarConsulta(consulta);

        // 2. Determinar repositorios a usar
        List<InfraccionesRepository> repositories = determinarRepositories(consulta.getParametrosFiltros());

        // 3. Ejecutar consultas (potencialmente en paralelo)
        List<Map<String, Object>> resultadosCombinados = ejecutarConsultasParalelas(
                repositories,
                consulta.getParametrosFiltros()
        );

        // 4. Convertir al formato solicitado
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    private List<InfraccionesRepository> determinarRepositories(ParametrosFiltrosDTO filtros) {
        if (filtros.getUsarTodasLasBDS() != null && filtros.getUsarTodasLasBDS()) {
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        } else if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            return filtros.getBaseDatos().stream()
                    .map(repositoryFactory::getRepository)
                    .collect(Collectors.toList());
        } else {
            // Default: usar todas
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        }
    }

    private List<Map<String, Object>> ejecutarConsultasParalelas(
            List<InfraccionesRepository> repositories,
            ParametrosFiltrosDTO filtros) {

        List<CompletableFuture<List<Map<String, Object>>>> futures = repositories.stream()
                .map(repo -> CompletableFuture.supplyAsync(
                        () -> repo.consultarPersonasJuridicas(filtros),
                        executor
                ))
                .collect(Collectors.toList());

        // Combinar todos los resultados
        return futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
