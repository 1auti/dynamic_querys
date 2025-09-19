package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConsolidacionService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    /**
     * Mapeo de códigos internos a nombres de provincias normalizados
     */
    private static final Map<String, String> MAPEO_PROVINCIAS = new HashMap<>();

    static {
        MAPEO_PROVINCIAS.put("Buenos Aires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("BuenosAires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("LaPampa", "La Pampa");
        MAPEO_PROVINCIAS.put("Chaco", "Chaco");
        MAPEO_PROVINCIAS.put("EntreRos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Entre Ríos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Formosa", "Formosa");
    }


    /**
     * Método principal SIMPLE para consolidar datos con orden específico
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación para {} provincias", repositories.size());

        // 1. Recopilar datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);

        if (todosLosDatos.isEmpty()) {
            log.info("No hay datos para consolidar");
            return Collections.emptyList();
        }

        log.info("Total datos recopilados: {} registros", todosLosDatos.size());

        // 2. Normalizar provincias en todos los registros
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

        // 3. PROCESAR CONSOLIDACIÓN POR ORDEN
        List<Map<String, Object>> datosConsolidados = procesarConsolidacionPorOrden(todosLosDatos, filtros);

        // 4. Aplicar límites de paginación
        return aplicarLimites(datosConsolidados, filtros);
    }

    // =============== CONSOLIDACIÓN POR ORDEN ===============

    /**
     * Procesa la consolidación según el orden especificado en 'consolidacion'
     */
    private List<Map<String, Object>> procesarConsolidacionPorOrden(
            List<Map<String, Object>> datos, ParametrosFiltrosDTO filtros) {

        // Obtener la lista de consolidación del filtro usando el método correcto
        List<String> ordenConsolidacion = filtros.getConsolidacionSeguro();

        if (ordenConsolidacion.isEmpty()) {
            log.info("No hay orden de consolidación especificado - usando detección automática");
            return consolidarAutomatico(datos);
        }

        log.info("Aplicando consolidación por orden: {}", ordenConsolidacion);

        // Separar campos de agrupación y campos numéricos según su posición
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();

        for (String campo : ordenConsolidacion) {
            if (esNumericoEnDatos(campo, datos)) {
                camposNumericos.add(campo);
            } else {
                camposAgrupacion.add(campo);
            }
        }

        log.info("Campos de agrupación identificados: {}", camposAgrupacion);
        log.info("Campos numéricos identificados: {}", camposNumericos);

        // Si no hay campos de agrupación, usar provincia por defecto
        if (camposAgrupacion.isEmpty()) {
            camposAgrupacion.add("provincia");
        }

        return consolidarPorCamposEspecificos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Consolida usando campos específicos de agrupación y numéricos
     */
    private List<Map<String, Object>> consolidarPorCamposEspecificos(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupoConsolidado = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoSimple(registro, camposAgrupacion, camposNumericos));

            // Sumar SOLO los campos numéricos especificados
            sumarCamposNumericos(grupoConsolidado, registro, camposNumericos);

            // Incrementar contador
            Integer contador = (Integer) grupoConsolidado.getOrDefault("registros_consolidados", 0);
            grupoConsolidado.put("registros_consolidados", contador + 1);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());

        log.info("Consolidación por orden completada: {} grupos consolidados", resultado.size());

        return resultado;
    }

    /**
     * Crea un grupo simple con solo los campos especificados
     */
    private Map<String, Object> crearGrupoSimple(Map<String, Object> registro,
                                                 List<String> camposAgrupacion,
                                                 List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Campos de agrupación (mantener valores originales)
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Campos numéricos (inicializar en 0)
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

//        // Metadatos básicos
//        grupo.put("consolidado", true);
//        grupo.put("registros_consolidados", 0);

        return grupo;
    }

    /**
     * Suma solo los campos numéricos especificados
     */
    private void sumarCamposNumericos(Map<String, Object> consolidado,
                                      Map<String, Object> registro,
                                      List<String> camposNumericos) {

        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);
            if (valorRegistro != null && esNumerico(valorRegistro)) {
                Long valorNumerico = ((Number) valorRegistro).longValue();

                Long valorActual = 0L;
                Object valorConsolidado = consolidado.get(campo);
                if (valorConsolidado != null && esNumerico(valorConsolidado)) {
                    valorActual = ((Number) valorConsolidado).longValue();
                }

                consolidado.put(campo, valorActual + valorNumerico);

                log.trace("Sumando campo {}: {} + {} = {}",
                        campo, valorActual, valorNumerico, valorActual + valorNumerico);
            }
        }
    }

    /**
     * Verifica si un campo es numérico analizando los datos reales
     */
    private boolean esNumericoEnDatos(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return false;

        int contadorNumerico = 0;
        int muestra = Math.min(20, datos.size());

        for (int i = 0; i < muestra; i++) {
            Object valor = datos.get(i).get(campo);
            if (valor != null && esNumerico(valor)) {
                contadorNumerico++;
            }
        }

        // Considerar numérico si >70% de la muestra son números
        return (double) contadorNumerico / muestra > 0.7;
    }

    /**
     * Consolidación automática cuando no hay orden específico
     */
    private List<Map<String, Object>> consolidarAutomatico(List<Map<String, Object>> datos) {
        // Detección automática simple - solo provincia y campos numéricos más comunes
        List<String> camposAgrupacion = Arrays.asList("provincia");
        List<String> camposNumericos = detectarCamposNumericosComunes(datos);

        return consolidarPorCamposEspecificos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Detecta campos numéricos comunes automáticamente
     */
    private List<String> detectarCamposNumericosComunes(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return new ArrayList<>();

        List<String> camposComunes = Arrays.asList(
                "total", "vehiculos", "motos", "cantidad", "infracciones",
                "formato_no_valido", "luz_roja", "senda"
        );

        return camposComunes.stream()
                .filter(campo -> datos.get(0).containsKey(campo))
                .filter(campo -> esNumericoEnDatos(campo, datos))
                .collect(Collectors.toList());
    }

    // =============== NORMALIZACIÓN DE PROVINCIAS ===============

    /**
     * Normaliza los nombres de provincias en todos los registros
     */
    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = MAPEO_PROVINCIAS.getOrDefault(provincia, provincia);

            // Actualizar todos los campos relacionados con provincia
            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        log.debug("Provincias normalizadas en {} registros", datos.size());
        return datos;
    }

    /**
     * Obtiene la provincia de un registro desde cualquier campo disponible
     */
    private String obtenerProvinciaDelRegistro(Map<String, Object> registro) {
        // Intentar diferentes campos donde puede estar la provincia
        String[] camposProvincia = {"provincia", "provincia_origen", "contexto"};

        for (String campo : camposProvincia) {
            Object valor = registro.get(campo);
            if (valor != null && !valor.toString().trim().isEmpty()) {
                return valor.toString().trim();
            }
        }

        return "SIN_PROVINCIA";
    }

    // =============== MÉTODOS UTILITARIOS SIMPLIFICADOS ===============

    /**
     * Recopila datos de todos los repositories disponibles
     */
    private List<Map<String, Object>> recopilarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar provincia de origen a cada registro
                    datosProvider.forEach(registro -> {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                    });
                    todosLosDatos.addAll(datosProvider);

                    log.debug("Provincia {}: {} registros obtenidos", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error obteniendo datos de provincia {}: {}", provincia, e.getMessage());
            }
        }

        log.info("Total recopilado: {} registros de {} provincias",
                todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    /**
     * Crea una clave única para el grupo basada en los campos de agrupación
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * Verifica si un valor es numérico
     */
    private boolean esNumerico(Object valor) {
        return valor instanceof Number;
    }

    /**
     * Aplica límites de paginación a los datos consolidados
     */
    private List<Map<String, Object>> aplicarLimites(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        log.debug("Aplicando límites: limite={}, offset={}, total={}", limite, offset, datos.size());

        // Aplicar offset
        if (offset > 0) {
            if (offset >= datos.size()) {
                return Collections.emptyList();
            }
            datos = datos.subList(offset, datos.size());
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        log.debug("Límites aplicados: {} registros finales", datos.size());
        return datos;
    }

    // =============== MÉTODOS PÚBLICOS DE UTILIDAD ===============

    /**
     * Valida si es posible realizar consolidación
     */
    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        if (!filtros.esConsolidado()) {
            return false;
        }

        Map<String, ?> repositories = repositoryFactory.getAllRepositories();
        boolean valido = !repositories.isEmpty();

        if (!valido) {
            log.warn("No hay repositorios disponibles para consolidación");
        }

        return valido;
    }

    /**
     * Obtiene información sobre el estado de la consolidación
     */
    public Map<String, Object> obtenerInfoConsolidacion() {
        Set<String> provinciasDisponibles = repositoryFactory.getProvinciasSoportadas();
        Map<String, ?> repositories = repositoryFactory.getAllRepositories();

        Map<String, Object> info = new HashMap<>();
        info.put("provincias_disponibles", new ArrayList<>(provinciasDisponibles));
        info.put("total_provincias", provinciasDisponibles.size());
        info.put("repositories_activos", repositories.size());
        info.put("consolidacion_habilitada", !repositories.isEmpty());
        info.put("tipo_consolidacion", "simple_por_orden");
        info.put("version", "3.0");

        return info;
    }

    /**
     * Genera respuesta optimizada según el formato solicitado
     */
    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            return Collections.emptyMap();
        }

        return datos; // Devolver directamente los datos consolidados
    }
}