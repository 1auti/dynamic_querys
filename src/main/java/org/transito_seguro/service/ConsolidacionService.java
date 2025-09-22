package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.QueryAnalyzer;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.utils.NormalizadorProvincias;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConsolidacionService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private QueryRegistryService queryRegistryService; // 🆕 NUEVA DEPENDENCIA

    // Constantes para análisis de datos (mantenidas como fallback)
    private static final int MIN_MUESTRA_NUMERICA = 3;
    private static final double UMBRAL_NUMERICO = 0.8;
    private static final int TAMAÑO_MUESTRA = 50;

    /**
     * 🚀 MÉTODO PRINCIPAL MEJORADO: Consolidación con Registry
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación con REGISTRY para {} provincias, query: {}",
                repositories.size(), nombreQuery);

        // 1. 🆕 NUEVO: Obtener análisis desde el Registry (BD H2)
        QueryAnalyzer.AnalisisConsolidacion analisis =
                queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);

        if (!analisis.isEsConsolidado()) {
            log.warn("Query '{}' NO es consolidable según Registry", nombreQuery);
            return recopilarDatosSinConsolidar(repositories, nombreQuery, filtros);
        }

        log.info("Query '{}' es CONSOLIDABLE según Registry:", nombreQuery);
        log.info("  📍 Campos Ubicación: {}", analisis.getCamposUbicacion());
        log.info("  🏷️ Campos Agrupación: {}", analisis.getCamposAgrupacion());
        log.info("  📊 Campos Numéricos: {}", analisis.getCamposNumericos());

        // 2. Recopilar datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);
        if (todosLosDatos.isEmpty()) {
            return Collections.emptyList();
        }

        // 3. Normalizar provincias
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

        // 4. 🆕 NUEVO: Procesamiento optimizado usando metadata del Registry
        List<Map<String, Object>> datosConsolidados = procesarConsolidacionConRegistry(
                todosLosDatos, filtros, analisis);

        // 5. Aplicar límites
        return aplicarLimites(datosConsolidados, filtros);
    }

    /**
     * 🎯 NUEVO: Procesamiento optimizado usando metadata del Registry
     */
    private List<Map<String, Object>> procesarConsolidacionConRegistry(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            QueryAnalyzer.AnalisisConsolidacion analisis) {

        // Determinar estrategia desde Registry + preferencias del usuario
        List<String> camposAgrupacion = determinarCamposAgrupacionConRegistry(filtros, analisis);
        List<String> camposNumericos = determinarCamposNumericosConRegistry(datos, analisis);

        log.info("Estrategia FINAL (Registry + Usuario):");
        log.info("  🎯 Agrupar por: {}", camposAgrupacion);
        log.info("  📊 Sumar: {}", camposNumericos);

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Determina campos de agrupación priorizando Registry sobre análisis dinámico
     */
    private List<String> determinarCamposAgrupacionConRegistry(ParametrosFiltrosDTO filtros,
                                                               QueryAnalyzer.AnalisisConsolidacion analisis) {

        List<String> camposAgrupacion = new ArrayList<>();
        List<String> consolidacionUsuario = filtros.getConsolidacionSeguro();

        if (!consolidacionUsuario.isEmpty()) {
            // Usuario especificó campos - validar contra el Registry
            log.info("Validando campos del usuario contra Registry: {}", consolidacionUsuario);

            for (String campo : consolidacionUsuario) {
                if (analisis.getCamposAgrupacion().contains(campo) ||
                        analisis.getCamposUbicacion().contains(campo)) {
                    camposAgrupacion.add(campo);
                    log.debug("  ✅ Campo '{}' válido", campo);
                } else {
                    log.warn("  ❌ Campo '{}' NO válido según Registry", campo);
                }
            }
        }

        // Si no hay campos válidos del usuario, usar Registry directamente
        if (camposAgrupacion.isEmpty()) {
            log.info("Usando estrategia del Registry directamente");

            // Prioridad: ubicación primero, luego categorización
            camposAgrupacion.addAll(analisis.getCamposUbicacion());

            // Agregar máximo 2 campos de agrupación adicionales (no ubicación, no tiempo)
            List<String> camposAdicionales = analisis.getCamposAgrupacion().stream()
                    .filter(campo -> !analisis.getCamposUbicacion().contains(campo))
                    .filter(campo -> !analisis.getCamposTiempo().contains(campo))
                    .limit(2)
                    .collect(Collectors.toList());

            camposAgrupacion.addAll(camposAdicionales);
        }

        // Garantizar que siempre haya "provincia"
        if (!camposAgrupacion.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        return camposAgrupacion.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Determina campos numéricos priorizando Registry sobre detección dinámica
     */
    private List<String> determinarCamposNumericosConRegistry(List<Map<String, Object>> datos,
                                                              QueryAnalyzer.AnalisisConsolidacion analisis) {

        Set<String> camposNumericos = new HashSet<>();

        // 1. 🆕 PRIORIDAD: Usar campos del Registry
        camposNumericos.addAll(analisis.getCamposNumericos());
        log.info("Campos numéricos desde Registry: {}", analisis.getCamposNumericos());

        // 2. FALLBACK: Solo si el Registry no tiene campos numéricos, detectar dinámicamente
        if (camposNumericos.isEmpty() && !datos.isEmpty()) {
            log.info("Registry sin campos numéricos, detectando dinámicamente...");

            Set<String> todosLosCampos = datos.get(0).keySet();

            for (String campo : todosLosCampos) {
                // Solo analizar campos no clasificados por el Registry
                if (!analisis.getTipoPorCampo().containsKey(campo) &&
                        esNumericoEnDatos(campo, datos)) {
                    camposNumericos.add(campo);
                    log.debug("Campo numérico detectado dinámicamente: {}", campo);
                }
            }
        }

        List<String> resultado = new ArrayList<>(camposNumericos);
        log.info("Campos numéricos FINALES: {}", resultado);
        return resultado;
    }

    /**
     * Consolidación por campos específicos - método optimizado (sin cambios)
     */
    private List<Map<String, Object>> consolidarPorCampos(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("No hay campos de agrupación, retornando datos sin consolidar");
            return datos;
        }

        log.info("Consolidando {} registros por campos: {}", datos.size(), camposAgrupacion);

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoNuevo(registro, camposAgrupacion, camposNumericos));

            sumarCamposNumericos(grupo, registro, camposNumericos);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());
        log.info("Consolidación completada: {} registros → {} grupos",
                datos.size(), resultado.size());

        return resultado;
    }

    // =============== MÉTODOS UTILITARIOS (sin cambios) ===============

    private Map<String, Object> crearGrupoNuevo(Map<String, Object> registro,
                                                List<String> camposAgrupacion,
                                                List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Copiar campos de agrupación
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Inicializar campos numéricos en 0
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    private void sumarCamposNumericos(Map<String, Object> grupo,
                                      Map<String, Object> registro,
                                      List<String> camposNumericos) {
        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);

            if (valorRegistro == null) continue;

            Long valorNumerico = convertirANumero(valorRegistro);
            if (valorNumerico == null) continue;

            Long valorActual = obtenerValorNumerico(grupo.get(campo));
            grupo.put(campo, valorActual + valorNumerico);
        }
    }

    // [Resto de métodos utilitarios sin cambios...]
    private Long obtenerValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

    private Long convertirANumero(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }

        if (valor instanceof String) {
            String str = valor.toString().trim();
            if (!str.isEmpty()) {
                try {
                    return Math.round(Double.parseDouble(str));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }

        return null;
    }

    private boolean esNumericoEnDatos(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return false;

        int contadorNumerico = 0;
        int contadorNoNulo = 0;
        int muestra = Math.min(TAMAÑO_MUESTRA, datos.size());

        for (int i = 0; i < muestra; i++) {
            Object valor = datos.get(i).get(campo);
            if (valor != null) {
                contadorNoNulo++;
                if (esValorNumerico(valor)) {
                    contadorNumerico++;
                }
            }
        }

        if (contadorNoNulo < MIN_MUESTRA_NUMERICA) return false;
        return (double) contadorNumerico / contadorNoNulo > UMBRAL_NUMERICO;
    }

    private boolean esValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return true;
        }

        if (valor instanceof String) {
            try {
                Double.parseDouble(valor.toString().trim());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return false;
    }

    private List<Map<String, Object>> recopilarDatosSinConsolidar(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Recopilando datos SIN consolidar (modo normal)");

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            try {
                List<Map<String, Object>> datos = repo.ejecutarQueryConFiltros(nombreQuery, filtros);
                if (datos != null && !datos.isEmpty()) {
                    datos.forEach(registro -> registro.put("provincia", repo.getProvincia()));
                    todosLosDatos.addAll(datos);
                }
            } catch (Exception e) {
                log.error("Error en provincia {}: {}", repo.getProvincia(), e.getMessage());
            }
        }

        return todosLosDatos;
    }

    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = NormalizadorProvincias.normalizar(provincia);

            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        return datos;
    }

    private String obtenerProvinciaDelRegistro(Map<String, Object> registro) {
        String[] camposProvincia = {"provincia", "provincia_origen", "contexto"};

        for (String campo : camposProvincia) {
            Object valor = registro.get(campo);
            if (valor != null && !valor.toString().trim().isEmpty()) {
                return valor.toString().trim();
            }
        }

        return "SIN_PROVINCIA";
    }

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
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                    }

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Provincia {}: {} registros", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error en provincia {}: {}", provincia, e.getMessage());
            }
        }

        log.info("Total recopilado: {} registros de {} provincias",
                todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    private List<Map<String, Object>> aplicarLimites(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        // Aplicar offset
        if (offset > 0 && offset < datos.size()) {
            datos = datos.subList(offset, datos.size());
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        return datos;
    }

    // =============== MÉTODOS PÚBLICOS (actualizados para usar Registry) ===============

    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        return filtros.esConsolidado() && !repositoryFactory.getAllRepositories().isEmpty();
    }

    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        return datos != null && !datos.isEmpty() ? datos : Collections.emptyList();
    }

    /**
     * 🆕 NUEVO: Método para validar si una query específica puede ser consolidada (usando Registry)
     */
    public boolean puedeSerConsolidada(String nombreQuery) {
        try {
            QueryAnalyzer.AnalisisConsolidacion analisis =
                    queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
            return analisis.isEsConsolidado();
        } catch (Exception e) {
            log.error("Error validando consolidación para query '{}': {}", nombreQuery, e.getMessage());
            return false;
        }
    }

    /**
     * 🆕 NUEVO: Obtener metadata completa de consolidación desde Registry
     */
    public QueryAnalyzer.AnalisisConsolidacion obtenerMetadataConsolidacion(String nombreQuery) {
        return queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
    }
}