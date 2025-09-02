package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
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

    private final Executor executor = Executors.newFixedThreadPool(10);

    /**
     * Método principal para consultar infracciones con filtros dinámicos
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    /**
     * Método genérico para consultar con cualquier query
     */
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        log.info("Iniciando consulta de infracciones con query: {}", nombreQuery);

        // 1. Validar consulta
        validator.validarConsulta(consulta);
        log.debug("Validación completada exitosamente");

        // 2. Determinar repositorios a usar
        List<InfraccionesRepository> repositories = determinarRepositories(consulta.getParametrosFiltros());
        log.debug("Usando {} repositorios: {}", repositories.size(),
                repositories.stream().map(repo -> ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) repo).getProvincia())
                        .collect(Collectors.toList()));

        // 3. Ejecutar consultas en paralelo
        List<Map<String, Object>> resultadosCombinados = ejecutarConsultasParalelas(
                repositories,
                consulta.getParametrosFiltros(),
                nombreQuery
        );
        log.info("Consulta completada. Total de registros: {}", resultadosCombinados.size());

        // 4. Convertir al formato solicitado
        String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
        return formatoConverter.convertir(resultadosCombinados, formato);
    }

    /**
     * Consulta específica para personas jurídicas
     */
    public Object consultarPersonasJuridicas(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "consultar_personas_juridicas.sql");
    }

    /**
     * Consulta para reporte general de infracciones
     */
    public Object consultarReporteGeneral(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_infracciones_general_dinamico.sql");
    }

    /**
     * Consulta para reporte de infracciones por equipos
     */
    public Object consultarInfraccionesPorEquipos(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_infracciones_por_equipos_dinamico.sql");
    }

    /**
     * Consulta para reporte de vehículos por municipio
     */
    public Object consultarVehiculosPorMunicipio(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_vehiculos_por_municipio.sql");
    }

    /**
     * Consulta para reporte de radar fijo por equipo
     */
    public Object consultarRadarFijoPorEquipo(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_radar_fijo_dinamico.sql");
    }

    /**
     * Consulta para reporte de semáforo por equipo
     */
    public Object consultarSemaforoPorEquipo(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_semaforo_dinamico.sql");
    }

    /**
     * Consulta para infracciones sin email por municipio
     */
    public Object consultarSinEmailPorMunicipio(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reprote_sin_email_por_municipio.sql");
    }

    /**
     * Consulta para verificar imágenes de radar
     */
    public Object verificarImagenesRadar(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "verificar_imagenes_radar.sql");
    }

    /**
     * Consulta para reporte detallado de infracciones
     */
    public Object consultarReporteDetallado(ConsultaQueryDTO consulta) throws ValidationException {
        return consultarInfracciones(consulta, "reporte_infracciones_detallado.sql");
    }

    /**
     * Determina qué repositorios usar según los filtros
     */
    private List<InfraccionesRepository> determinarRepositories(ParametrosFiltrosDTO filtros) {
        if (filtros.debeConsultarTodasLasBDS()) {
            log.debug("Usando todas las bases de datos disponibles");
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        } else if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            log.debug("Usando bases de datos específicas: {}", filtros.getBaseDatos());
            return filtros.getBaseDatos().stream()
                    .filter(repositoryFactory::isProvinciaSupported)
                    .map(repositoryFactory::getRepository)
                    .collect(Collectors.toList());
        } else {
            // Default: usar todas si no se especifica
            log.debug("No se especificaron bases de datos, usando todas por defecto");
            return new ArrayList<>(repositoryFactory.getAllRepositories().values());
        }
    }

    /**
     * Ejecuta consultas en paralelo en múltiples repositorios
     */
    private List<Map<String, Object>> ejecutarConsultasParalelas(
            List<InfraccionesRepository> repositories,
            ParametrosFiltrosDTO filtros,
            String nombreQuery) {

        if (repositories.isEmpty()) {
            log.warn("No hay repositorios disponibles para la consulta");
            return new ArrayList<>();
        }

        // Crear futures para ejecutión paralela
        List<CompletableFuture<List<Map<String, Object>>>> futures = repositories.stream()
                .map(repo -> CompletableFuture.supplyAsync(() -> {
                    try {
                        String provincia = ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) repo).getProvincia();
                        log.debug("Ejecutando consulta en provincia: {}", provincia);

                        List<Map<String, Object>> resultado =
                                ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) repo)
                                        .ejecutarQueryConFiltros(nombreQuery, filtros);

                        // Agregar información de provincia a cada registro
                        resultado.forEach(registro -> {
                            if (!registro.containsKey("provincia_origen")) {
                                registro.put("provincia_origen", provincia);
                            }
                        });

                        log.debug("Consulta completada en provincia {}: {} registros", provincia, resultado.size());
                        return resultado;

                    } catch (Exception e) {
                        String provincia = ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) repo).getProvincia();
                        log.error("Error ejecutando consulta en provincia {}: {}", provincia, e.getMessage(), e);
                        return new ArrayList<Map<String, Object>>();
                    }
                }, executor))
                .collect(Collectors.toList());

        // Combinar todos los resultados
        List<Map<String, Object>> resultadosCombinados = futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        log.info("Consultas paralelas completadas. Total combinado: {} registros", resultadosCombinados.size());
        return resultadosCombinados;
    }

    /**
     * Método para obtener estadísticas de consulta
     */
    public Map<String, Object> obtenerEstadisticasConsulta(ParametrosFiltrosDTO filtros) {
        List<InfraccionesRepository> repositories = determinarRepositories(filtros);

        Map<String, Object> filtrosAplicados = new HashMap<>();
        filtrosAplicados.put("fecha", filtros.debeUsarFechaEspecifica() || filtros.tieneRangoFechas());
        filtrosAplicados.put("ubicacion", filtros.tieneFiltrosUbicacion());
        filtrosAplicados.put("equipos", filtros.tieneFiltrosEquipos());
        filtrosAplicados.put("infracciones", filtros.tieneFiltrosInfracciones());

        List<String> provinciasSeleccionadas = repositories.stream()
                .map(repo -> ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) repo).getProvincia())
                .collect(Collectors.toList());

        Map<String, Object> estadisticas = new HashMap<>();
        estadisticas.put("repositoriosDisponibles", repositoryFactory.getProvinciasSoportadas().size());
        estadisticas.put("repositoriosSeleccionados", repositories.size());
        estadisticas.put("provinciasSeleccionadas", provinciasSeleccionadas);
        estadisticas.put("filtrosAplicados", filtrosAplicados);


        return estadisticas;
    }

    /**
     * Método para validar conectividad de repositorios
     */
    public Map<String, Boolean> validarConectividadRepositorios() {
        Map<String, Boolean> conectividad = repositoryFactory.getAllRepositories().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                // Ejecutar una query simple para validar conectividad
                                ParametrosFiltrosDTO filtrosTest = ParametrosFiltrosDTO.builder()
                                        .limiteMaximo(1)
                                        .build();

                                ((org.transito_seguro.repository.impl.InfraccionesRepositoryImpl) entry.getValue())
                                        .ejecutarQueryConFiltros("SELECT 1 as test", filtrosTest);
                                return true;
                            } catch (Exception e) {
                                log.warn("Repositorio {} no disponible: {}", entry.getKey(), e.getMessage());
                                return false;
                            }
                        }
                ));

        return conectividad;
    }

    /**
     * Método utilitario para construir consultas con filtros específicos
     */
    public ConsultaQueryDTO.ConsultaQueryDTOBuilder crearConsultaBase() {
        return ConsultaQueryDTO.builder()
                .formato("json")
                .parametrosFiltros(
                        ParametrosFiltrosDTO.builder()
                                .usarTodasLasBDS(true)
                                .pagina(0)
                                .tamanoPagina(100)
                                .direccionOrdenamiento("ASC")
                                .incluirEliminados(false)
                                .build()
                );
    }

}