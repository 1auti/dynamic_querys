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

/**
 * Servicio especializado en consolidación automática de datos multi-provincia.
 *
 * Este servicio maneja:
 * - Análisis automático de queries para determinar consolidabilidad
 * - Agrupación inteligente por campos geográficos y de categorización
 * - Suma automática de campos numéricos
 * - Normalización de datos entre provincias
 * - Integración con el sistema QueryRegistry para metadata optimizada
 *
 * La consolidación permite combinar datos de múltiples provincias en reportes
 * unificados, eliminando duplicados y sumando métricas numéricas.
 */
@Slf4j
@Service
public class ConsolidacionService {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private QueryRegistryService queryRegistryService;

    // =============== CONFIGURACIÓN DE ANÁLISIS ===============

    /** Mínimo de registros no nulos para considerar un campo como numérico */
    private static final int MIN_MUESTRA_NUMERICA = 3;

    /** Umbral de valores numéricos para clasificar un campo como numérico (80%) */
    private static final double UMBRAL_NUMERICO = 0.8;

    /** Tamaño de muestra para análisis dinámico de campos */
    private static final int TAMAÑO_MUESTRA = 50;

    // =============== API PRINCIPAL ===============

    /**
     * Consolida datos de múltiples provincias usando análisis automático desde QueryRegistry.
     *
     * Este método:
     * 1. Obtiene metadata de consolidación desde QueryRegistry (BD H2)
     * 2. Recopila datos de todas las provincias especificadas
     * 3. Normaliza nombres de provincias para consistencia
     * 4. Agrupa y suma datos según los campos identificados automáticamente
     * 5. Aplica límites y paginación
     *
     * @param repositories Lista de repositorios de provincias a consultar
     * @param nombreQuery Código de la query en el registry
     * @param filtros Parámetros de filtrado y configuración de consolidación
     * @return List<Map<String, Object>> Datos consolidados con métricas sumadas
     * @throws RuntimeException si hay errores durante el procesamiento
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("=== Iniciando consolidación automática ===");
        log.info("Provincias: {}, Query: {}, Filtros consolidación: {}",
                repositories.size(), nombreQuery, filtros.getConsolidacionSeguro());

        // 1. Obtener análisis de consolidación desde QueryRegistry
        QueryAnalyzer.AnalisisConsolidacion analisis = obtenerAnalisisConsolidacion(nombreQuery);

        if (!analisis.isEsConsolidado()) {
            log.warn("Query '{}' NO es consolidable según Registry - retornando datos sin consolidar", nombreQuery);
            return recopilarDatosSinConsolidar(repositories, nombreQuery, filtros);
        }

        logAnalisisConsolidacion(nombreQuery, analisis);

        // 2. Recopilar y procesar datos
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);
        if (todosLosDatos.isEmpty()) {
            log.info("No se encontraron datos para consolidar");
            return Collections.emptyList();
        }

        // 3. Normalizar y consolidar
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);
        List<Map<String, Object>> datosConsolidados = procesarConsolidacion(todosLosDatos, filtros, analisis);

        // 4. Aplicar límites finales
        List<Map<String, Object>> resultado = aplicarLimites(datosConsolidados, filtros);

        log.info("=== Consolidación completada: {} registros finales ===", resultado.size());
        return resultado;
    }

    /**
     * Valida si una consulta puede ser consolidada.
     *
     * @param filtros Parámetros que deben incluir flag de consolidación
     * @return boolean true si la consolidación es posible y está solicitada
     */
    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        boolean esConsolidado = filtros.esConsolidado();
        boolean hayRepositorios = !repositoryFactory.getAllRepositories().isEmpty();

        log.debug("Validación consolidación: solicitada={}, repositorios disponibles={}",
                esConsolidado, hayRepositorios);

        return esConsolidado && hayRepositorios;
    }

    /**
     * Valida si una query específica puede ser consolidada consultando el Registry.
     *
     * @param nombreQuery Código de la query a validar
     * @return boolean true si la query es consolidable según su metadata
     */
    public boolean puedeSerConsolidada(String nombreQuery) {
        try {
            QueryAnalyzer.AnalisisConsolidacion analisis =
                    queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);

            boolean consolidable = analisis.isEsConsolidado();
            log.debug("Query '{}' es consolidable: {}", nombreQuery, consolidable);

            return consolidable;

        } catch (Exception e) {
            log.error("Error validando consolidación para query '{}': {}", nombreQuery, e.getMessage());
            return false;
        }
    }

    /**
     * Genera respuesta optimizada para datos consolidados.
     *
     * @param datos Datos consolidados a formatear
     * @param formato Formato de salida solicitado
     * @return Object Datos en el formato apropiado, lista vacía si no hay datos
     */
    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            log.debug("Generando respuesta vacía para formato: {}", formato);
            return Collections.emptyList();
        }

        log.debug("Generando respuesta consolidada: {} registros, formato: {}", datos.size(), formato);
        return datos;
    }

    // =============== PROCESAMIENTO INTERNO ===============

    /**
     * Obtiene análisis de consolidación desde el QueryRegistry con manejo de errores.
     *
     * @param nombreQuery Código de la query
     * @return QueryAnalyzer.AnalisisConsolidacion Análisis con metadata de consolidación
     */
    private QueryAnalyzer.AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        try {
            return queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
        } catch (Exception e) {
            log.error("Error obteniendo análisis para query '{}': {}", nombreQuery, e.getMessage());
            // Retornar análisis vacío como fallback
            return new QueryAnalyzer.AnalisisConsolidacion(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    false
            );
        }
    }

    /**
     * Registra información detallada del análisis de consolidación.
     *
     * @param nombreQuery Código de la query
     * @param analisis Análisis obtenido del Registry
     */
    private void logAnalisisConsolidacion(String nombreQuery, QueryAnalyzer.AnalisisConsolidacion analisis) {
        log.info("Query '{}' es CONSOLIDABLE según Registry:", nombreQuery);
        log.info("   Campos Ubicación: {}", analisis.getCamposUbicacion());
        log.info("  ️ Campos Agrupación: {}", analisis.getCamposAgrupacion());
        log.info("   Campos Numéricos: {}", analisis.getCamposNumericos());
        log.info("   Campos Tiempo: {}", analisis.getCamposTiempo());
    }

    /**
     * Procesa la consolidación usando metadata del Registry y preferencias del usuario.
     *
     * @param datos Datos a consolidar
     * @param filtros Filtros con preferencias del usuario
     * @param analisis Metadata de consolidación del Registry
     * @return List<Map<String, Object>> Datos consolidados
     */
    private List<Map<String, Object>> procesarConsolidacion(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            QueryAnalyzer.AnalisisConsolidacion analisis) {

        // Determinar estrategia de consolidación
        List<String> camposAgrupacion = determinarCamposAgrupacion(filtros, analisis);
        List<String> camposNumericos = determinarCamposNumericos(datos, analisis);

        log.info("Estrategia de consolidación aplicada:");
        log.info("   Agrupar por: {}", camposAgrupacion);
        log.info("   Sumar: {}", camposNumericos);

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Determina campos de agrupación priorizando Registry sobre preferencias del usuario.
     *
     * @param filtros Filtros con preferencias del usuario
     * @param analisis Metadata del Registry
     * @return List<String> Campos de agrupación finales validados
     */
    private List<String> determinarCamposAgrupacion(ParametrosFiltrosDTO filtros,
                                                    QueryAnalyzer.AnalisisConsolidacion analisis) {

        List<String> camposAgrupacion = new ArrayList<>();
        List<String> preferenciasUsuario = filtros.getConsolidacionSeguro();

        if (!preferenciasUsuario.isEmpty()) {
            // Validar preferencias del usuario contra campos válidos del Registry
            log.info("Validando preferencias del usuario: {}", preferenciasUsuario);

            Set<String> camposValidos = new HashSet<>();
            camposValidos.addAll(analisis.getCamposAgrupacion());
            camposValidos.addAll(analisis.getCamposUbicacion());

            for (String campo : preferenciasUsuario) {
                if (camposValidos.contains(campo)) {
                    camposAgrupacion.add(campo);
                    log.debug("  ✅ Campo '{}' válido", campo);
                } else {
                    log.warn("  ❌ Campo '{}' NO válido según Registry", campo);
                }
            }
        }

        // Fallback: usar estrategia automática del Registry
        if (camposAgrupacion.isEmpty()) {
            log.info("Aplicando estrategia automática del Registry");

            // Prioridad: ubicación -> categorización (limitada)
            camposAgrupacion.addAll(analisis.getCamposUbicacion());

            // Agregar máximo 2 campos adicionales de categorización
            List<String> camposCategorizacion = analisis.getCamposAgrupacion().stream()
                    .filter(campo -> !analisis.getCamposUbicacion().contains(campo))
                    .filter(campo -> !analisis.getCamposTiempo().contains(campo))
                    .limit(2)
                    .collect(Collectors.toList());

            camposAgrupacion.addAll(camposCategorizacion);
        }

        // Garantizar que siempre incluya "provincia"
        if (!camposAgrupacion.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        return camposAgrupacion.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Determina campos numéricos priorizando Registry sobre detección dinámica.
     *
     * @param datos Datos para análisis dinámico (fallback)
     * @param analisis Metadata del Registry
     * @return List<String> Campos numéricos identificados
     */
    private List<String> determinarCamposNumericos(List<Map<String, Object>> datos,
                                                   QueryAnalyzer.AnalisisConsolidacion analisis) {

        Set<String> camposNumericos = new HashSet<>();

        // 1. PRIORIDAD: Campos numéricos del Registry
        camposNumericos.addAll(analisis.getCamposNumericos());
        log.info("Campos numéricos desde Registry: {}", analisis.getCamposNumericos());

        // 2. FALLBACK: Detección dinámica solo si Registry no tiene campos numéricos
        if (camposNumericos.isEmpty() && !datos.isEmpty()) {
            log.info("Registry sin campos numéricos - ejecutando detección dinámica");

            Set<String> todosLosCampos = datos.get(0).keySet();
            Map<String, ?> tiposPorCampo = analisis.getTipoPorCampo();

            for (String campo : todosLosCampos) {
                // Solo analizar campos no clasificados por el Registry
                if (!tiposPorCampo.containsKey(campo) && esNumericoEnDatos(campo, datos)) {
                    camposNumericos.add(campo);
                    log.debug("Campo numérico detectado dinámicamente: {}", campo);
                }
            }
        }

        List<String> resultado = new ArrayList<>(camposNumericos);
        log.info("Campos numéricos FINALES: {}", resultado);
        return resultado;
    }

    // =============== PROCESAMIENTO DE DATOS ===============

    /**
     * Consolida datos agrupando por campos especificados y sumando campos numéricos.
     *
     * @param datos Datos a consolidar
     * @param camposAgrupacion Campos para crear grupos únicos
     * @param camposNumericos Campos numéricos a sumar por grupo
     * @return List<Map<String, Object>> Datos consolidados con métricas sumadas
     */
    private List<Map<String, Object>> consolidarPorCampos(List<Map<String, Object>> datos,
                                                          List<String> camposAgrupacion,
                                                          List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("Sin campos de agrupación - retornando datos originales");
            return datos;
        }

        log.info("Procesando consolidación: {} registros → agrupación por {}",
                datos.size(), camposAgrupacion);

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();
        int registrosProcesados = 0;

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> inicializarNuevoGrupo(registro, camposAgrupacion, camposNumericos));

            acumularCamposNumericos(grupo, registro, camposNumericos);

            registrosProcesados++;

            // Log de progreso cada 10,000 registros
            if (registrosProcesados % 10000 == 0) {
                log.debug("Procesados {} registros, {} grupos creados",
                        registrosProcesados, grupos.size());
            }
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());
        log.info("Consolidación completada: {} registros → {} grupos únicos",
                datos.size(), resultado.size());

        return resultado;
    }

    /**
     * Inicializa un nuevo grupo copiando campos de agrupación e inicializando campos numéricos.
     *
     * @param registro Registro base para copiar valores de agrupación
     * @param camposAgrupacion Campos a copiar del registro original
     * @param camposNumericos Campos numéricos a inicializar en 0
     * @return Map<String, Object> Nuevo grupo inicializado
     */
    private Map<String, Object> inicializarNuevoGrupo(Map<String, Object> registro,
                                                      List<String> camposAgrupacion,
                                                      List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Copiar valores de los campos de agrupación
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Inicializar campos numéricos en 0
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    /**
     * Acumula valores de campos numéricos de un registro a un grupo existente.
     *
     * @param grupo Grupo acumulador
     * @param registro Registro con valores a sumar
     * @param camposNumericos Campos numéricos a procesar
     */
    private void acumularCamposNumericos(Map<String, Object> grupo,
                                         Map<String, Object> registro,
                                         List<String> camposNumericos) {
        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);
            if (valorRegistro == null) continue;

            Long valorNumerico = convertirALong(valorRegistro);
            if (valorNumerico == null) continue;

            Long valorActual = obtenerValorLong(grupo.get(campo));
            grupo.put(campo, valorActual + valorNumerico);
        }
    }

    /**
     * Crea clave única para agrupar concatenando valores de campos de agrupación.
     *
     * @param registro Registro a procesar
     * @param camposAgrupacion Campos para generar la clave
     * @return String Clave única del grupo
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    // =============== RECOPILACIÓN Y NORMALIZACIÓN ===============

    /**
     * Recopila datos de todos los repositorios sin aplicar consolidación.
     *
     * @param repositories Lista de repositorios de provincias
     * @param nombreQuery Código de la query a ejecutar
     * @param filtros Parámetros de filtrado
     * @return List<Map<String, Object>> Datos combinados sin consolidar
     */
    private List<Map<String, Object>> recopilarDatosSinConsolidar(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Recopilando datos SIN consolidación de {} provincias", repositories.size());
        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            try {
                List<Map<String, Object>> datos = repo.ejecutarQueryConFiltros(nombreQuery, filtros);
                if (datos != null && !datos.isEmpty()) {
                    // Agregar metadata de provincia
                    datos.forEach(registro -> registro.put("provincia", repo.getProvincia()));
                    todosLosDatos.addAll(datos);
                }
            } catch (Exception e) {
                log.error("Error recopilando datos de provincia {}: {}",
                        repo.getProvincia(), e.getMessage());
            }
        }

        log.info("Recopilación sin consolidar completada: {} registros totales", todosLosDatos.size());
        return todosLosDatos;
    }

    /**
     * Recopila datos de todos los repositorios para consolidación posterior.
     *
     * @param repositories Lista de repositorios de provincias
     * @param nombreQuery Código de la query a ejecutar
     * @param filtros Parámetros de filtrado
     * @return List<Map<String, Object>> Datos combinados listos para consolidar
     */
    private List<Map<String, Object>> recopilarDatos(List<InfraccionesRepositoryImpl> repositories,
                                                     String nombreQuery,
                                                     ParametrosFiltrosDTO filtros) {

        log.info("Recopilando datos para consolidación de {} provincias", repositories.size());
        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar metadata de provincia y origen
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                    }

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Provincia {}: {} registros recopilados", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error recopilando datos de provincia {}: {}", provincia, e.getMessage());
            }
        }

        log.info("Recopilación completada: {} registros de {} provincias",
                todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    /**
     * Normaliza nombres de provincias en los datos para asegurar consistencia.
     *
     * @param datos Datos con campos de provincia a normalizar
     * @return List<Map<String, Object>> Datos con provincias normalizadas
     */
    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        log.debug("Normalizando nombres de provincias en {} registros", datos.size());

        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = NormalizadorProvincias.normalizar(provincia);

            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        return datos;
    }

    /**
     * Obtiene el nombre de provincia de un registro buscando en múltiples campos.
     *
     * @param registro Registro a examinar
     * @return String Nombre de provincia encontrado o "SIN_PROVINCIA"
     */
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

    /**
     * Aplica límites de paginación y cantidad de registros a los datos consolidados.
     *
     * @param datos Datos consolidados
     * @param filtros Filtros con configuración de límites
     * @return List<Map<String, Object>> Datos con límites aplicados
     */
    private List<Map<String, Object>> aplicarLimites(List<Map<String, Object>> datos,
                                                     ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        log.debug("Aplicando límites - Offset: {}, Límite: {}, Datos disponibles: {}",
                offset, limite, datos.size());

        // Aplicar offset
        if (offset > 0 && offset < datos.size()) {
            datos = datos.subList(offset, datos.size());
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        log.debug("Límites aplicados - Registros finales: {}", datos.size());
        return datos;
    }

    // =============== UTILIDADES DE CONVERSIÓN ===============

    /**
     * Convierte un valor a Long manejando diferentes tipos de datos.
     *
     * @param valor Valor a convertir (Number, String, etc.)
     * @return Long Valor convertido o null si no es convertible
     */
    private Long convertirALong(Object valor) {
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

    /**
     * Obtiene valor Long de un objeto, retorna 0 si no es convertible.
     *
     * @param valor Valor a convertir
     * @return Long Valor numérico o 0
     */
    private Long obtenerValorLong(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

    // =============== ANÁLISIS DINÁMICO DE CAMPOS ===============

    /**
     * Determina si un campo es numérico analizando una muestra de sus valores.
     *
     * @param campo Nombre del campo a analizar
     * @param datos Lista de datos para el análisis
     * @return boolean true si el campo es considerado numérico
     */
    private boolean esNumericoEnDatos(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return false;
        }

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

        // Requiere mínimo de valores no nulos y porcentaje de valores numéricos
        if (contadorNoNulo < MIN_MUESTRA_NUMERICA) {
            return false;
        }

        double porcentajeNumerico = (double) contadorNumerico / contadorNoNulo;
        boolean esNumerico = porcentajeNumerico > UMBRAL_NUMERICO;

        log.debug("Análisis campo '{}': {}/{} valores numéricos ({}%) -> {}",
                campo, contadorNumerico, contadorNoNulo,
                String.format("%.1f", porcentajeNumerico * 100), esNumerico);

        return esNumerico;
    }

    /**
     * Verifica si un valor individual puede ser tratado como numérico.
     *
     * @param valor Valor a verificar
     * @return boolean true si el valor es numérico
     */
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
}