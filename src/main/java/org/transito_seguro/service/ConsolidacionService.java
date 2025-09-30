package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.utils.NormalizadorProvincias;

import java.util.*;
import java.util.stream.Collectors;

import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;

/**
 * Servicio especializado en consolidación automática de datos multi-provincia MEJORADO.
 *
 * NUEVAS CARACTERÍSTICAS:
 * - Consolidación jerárquica inteligente basada en prioridades del usuario
 * - Agrupación dinámica que respeta el orden de campos solicitados
 * - Soporte para consolidación por ubicación geográfica automática
 * - Detección automática de campos numéricos en tiempo real
 * - Consolidación adaptativa según el contexto de los datos
 */
@Slf4j
@Service
public class ConsolidacionService {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private QueryRegistryService queryRegistryService;

    // =============== CONFIGURACIÓN MEJORADA ===============

    /** Mínimo de registros no nulos para considerar un campo como numérico */
    private static final int MIN_MUESTRA_NUMERICA = 3;

    /** Umbral de valores numéricos para clasificar un campo como numérico (80%) */
    private static final double UMBRAL_NUMERICO = 0.8;

    /** Tamaño de muestra para análisis dinámico de campos */
    private static final int TAMAÑO_MUESTRA = 50;

    // =============== ENUMS PARA ESTRATEGIAS ===============

    public enum EstrategiaConsolidacion {
        JERARQUICA_USUARIO,      // Prioriza campos solicitados por usuario
        GEOGRAFICA_AUTOMATICA,   // Agrupa por ubicación geográfica
        INTELIGENTE_MIXTA        // Combina ambas estrategias
    }

    public enum TipoAgrupacion {
        GEOGRAFICA,     // provincia, municipio, lugar
        TEMPORAL,       // fecha, mes, año
        CATEGORICA,     // descripcion, estado, tipo
        NUMERICA        // campos de conteo/suma
    }

    // =============== API PRINCIPAL MEJORADA ===============

    /**
     * 🚀 MÉTODO PRINCIPAL MEJORADO: Consolida datos con estrategia inteligente
     *
     * MEJORAS IMPLEMENTADAS:
     * 1. Respeta el ORDEN de campos solicitados por el usuario
     * 2. Implementa consolidación JERÁRQUICA (descripcion + provincia + lugar)
     * 3. Detecta automáticamente campos numéricos en tiempo real
     * 4. Aplica estrategias adaptativas según el tipo de datos
     * 5. Mantiene compatibilidad con el sistema existente
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("=== CONSOLIDACIÓN MEJORADA INICIADA ===");
        log.info("Provincias: {}, Query: {}, Consolidación solicitada: {}",
                repositories.size(), nombreQuery, filtros.getConsolidacionSeguro());

        // 1. Determinar estrategia de consolidación basada en solicitud del usuario
        EstrategiaConsolidacion estrategia = determinarEstrategia(filtros);
        log.info("Estrategia seleccionada: {}", estrategia);

        // 2. Obtener análisis de consolidación (metadata del Registry)
        AnalisisConsolidacion analisis = obtenerAnalisisConsolidacion(nombreQuery);

        // 3. Recopilar datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);
        if (todosLosDatos.isEmpty()) {
            log.info("No se encontraron datos para consolidar");
            return Collections.emptyList();
        }

        // 4. Normalizar provincias para consistencia
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

        // 5. NUEVA LÓGICA: Aplicar consolidación según estrategia determinada
        List<Map<String, Object>> datosConsolidados = aplicarEstrategiaConsolidacion(
                todosLosDatos, filtros, analisis, estrategia);

        // 6. Aplicar límites finales
        List<Map<String, Object>> resultado = aplicarLimites(datosConsolidados, filtros);

        log.info("=== CONSOLIDACIÓN COMPLETADA: {} registros finales ===", resultado.size());
        return resultado;
    }

    // =============== NUEVOS MÉTODOS: ESTRATEGIAS DE CONSOLIDACIÓN ===============

    /**
     * 🧠 Determina la estrategia de consolidación basada en la solicitud del usuario
     */
    private EstrategiaConsolidacion determinarEstrategia(ParametrosFiltrosDTO filtros) {
        List<String> camposUsuario = filtros.getConsolidacionSeguro();

        if (camposUsuario.isEmpty()) {
            return EstrategiaConsolidacion.GEOGRAFICA_AUTOMATICA;
        }

        // Si el usuario especificó campos, priorizar su solicitud
        boolean tieneGeograficos = camposUsuario.stream()
                .anyMatch(campo -> esGeografico(campo));

        boolean tieneCategoricos = camposUsuario.stream()
                .anyMatch(campo -> esCategorico(campo));

        if (tieneGeograficos && tieneCategoricos) {
            return EstrategiaConsolidacion.INTELIGENTE_MIXTA;
        } else if (camposUsuario.size() > 1) {
            return EstrategiaConsolidacion.JERARQUICA_USUARIO;
        } else {
            return EstrategiaConsolidacion.GEOGRAFICA_AUTOMATICA;
        }
    }

    /**
     * 🎯 Aplica la estrategia de consolidación determinada
     */
    private List<Map<String, Object>> aplicarEstrategiaConsolidacion(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            AnalisisConsolidacion analisis,
            EstrategiaConsolidacion estrategia) {

        switch (estrategia) {
            case JERARQUICA_USUARIO:
                return aplicarConsolidacionJerarquicaUsuario(datos, filtros, analisis);

            case GEOGRAFICA_AUTOMATICA:
                return aplicarConsolidacionGeograficaAutomatica(datos, analisis);

            case INTELIGENTE_MIXTA:
                return aplicarConsolidacionInteligenteMixta(datos, filtros, analisis);

            default:
                log.warn("Estrategia no reconocida, usando consolidación por defecto");
                return consolidarPorCamposTradicional(datos,
                        determinarCamposAgrupacionLegacy(filtros, analisis),
                        determinarCamposNumericosLegacy(datos, analisis));
        }
    }

    // =============== IMPLEMENTACIÓN DE ESTRATEGIAS ===============

    /**
     * 🏗️ NUEVA ESTRATEGIA: Consolidación Jerárquica Usuario
     *
     * Ejemplo: usuario solicita ["descripcion", "provincia"]
     * Resultado: Agrupa por descripcion + provincia + lugar (si existe)
     */
    private List<Map<String, Object>> aplicarConsolidacionJerarquicaUsuario(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            AnalisisConsolidacion analisis) {

        log.info("🏗️ Aplicando CONSOLIDACIÓN JERÁRQUICA USUARIO");

        List<String> camposUsuario = new ArrayList<>(filtros.getConsolidacionSeguro());

        // PASO 1: Validar que los campos solicitados existen en los datos
        Set<String> camposDisponibles = datos.isEmpty() ?
                Collections.emptySet() : datos.get(0).keySet();

        List<String> camposValidos = camposUsuario.stream()
                .filter(camposDisponibles::contains)
                .collect(Collectors.toList());

        if (camposValidos.isEmpty()) {
            log.warn("Ningún campo solicitado por el usuario está disponible en los datos");
            return aplicarConsolidacionGeograficaAutomatica(datos, analisis);
        }

        // PASO 2: Agregar campos de ubicación automáticamente si no están
        List<String> camposFinales = new ArrayList<>(camposValidos);
        agregarCamposUbicacionSiNecesario(camposFinales, camposDisponibles);

        // PASO 3: Detectar campos numéricos dinámicamente
        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Consolidación jerárquica - Agrupación: {}, Numéricos: {}",
                camposFinales, camposNumericos);

        // PASO 4: Aplicar consolidación con los campos ordenados
        return consolidarPorCamposOrdenados(datos, camposFinales, camposNumericos);
    }

    /**
     * 🌍 NUEVA ESTRATEGIA: Consolidación Geográfica Automática
     */
    private List<Map<String, Object>> aplicarConsolidacionGeograficaAutomatica(
            List<Map<String, Object>> datos,
            AnalisisConsolidacion analisis) {

        log.info("🌍 Aplicando CONSOLIDACIÓN GEOGRÁFICA AUTOMÁTICA");

        // Priorizar campos geográficos en orden lógico
        List<String> camposGeograficos = Arrays.asList(
                "provincia", "municipio", "lugar", "contexto", "partido"
        );

        Set<String> camposDisponibles = datos.isEmpty() ?
                Collections.emptySet() : datos.get(0).keySet();

        List<String> camposAgrupacion = camposGeograficos.stream()
                .filter(camposDisponibles::contains)
                .limit(3) // Máximo 3 niveles geográficos
                .collect(Collectors.toList());

        // Asegurar que siempre haya al menos "provincia"
        if (!camposAgrupacion.contains("provincia") && camposDisponibles.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Consolidación geográfica - Agrupación: {}, Numéricos: {}",
                camposAgrupacion, camposNumericos);

        return consolidarPorCamposOrdenados(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * 🧠 NUEVA ESTRATEGIA: Consolidación Inteligente Mixta
     */
    private List<Map<String, Object>> aplicarConsolidacionInteligenteMixta(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            AnalisisConsolidacion analisis) {

        log.info("🧠 Aplicando CONSOLIDACIÓN INTELIGENTE MIXTA");

        List<String> camposUsuario = filtros.getConsolidacionSeguro();
        Set<String> camposDisponibles = datos.isEmpty() ?
                Collections.emptySet() : datos.get(0).keySet();

        // Clasificar campos del usuario por tipo
        List<String> camposGeograficos = new ArrayList<>();
        List<String> camposCategoricos = new ArrayList<>();

        for (String campo : camposUsuario) {
            if (camposDisponibles.contains(campo)) {
                if (esGeografico(campo)) {
                    camposGeograficos.add(campo);
                } else if (esCategorico(campo)) {
                    camposCategoricos.add(campo);
                }
            }
        }

        // Construir jerarquía: Categóricos → Geográficos → Ubicación adicional
        List<String> camposFinales = new ArrayList<>();
        camposFinales.addAll(camposCategoricos);  // Prioridad a campos categóricos del usuario
        camposFinales.addAll(camposGeograficos);  // Luego geográficos del usuario

        // Agregar campos de ubicación adicionales si no están
        agregarCamposUbicacionSiNecesario(camposFinales, camposDisponibles);

        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Consolidación mixta - Agrupación: {}, Numéricos: {}",
                camposFinales, camposNumericos);

        return consolidarPorCamposOrdenados(datos, camposFinales, camposNumericos);
    }

    // =============== MÉTODOS AUXILIARES NUEVOS ===============

    /**
     * 🔍 Detecta automáticamente campos numéricos en los datos
     * MEJORADO: Maneja funciones SQL como COUNT(*), SUM(campo), etc.
     */
    private List<String> detectarCamposNumericosDinamicos(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> todosLosCampos = datos.get(0).keySet();
        List<String> camposNumericos = new ArrayList<>();

        for (String campo : todosLosCampos) {
            boolean esNumericoPorNombre = org.transito_seguro.utils.SqlFieldDetector.esNumericoPorNombre(campo);
            boolean esNumericoPorDatos = esNumericoEnDatos(campo, datos);

            if (esNumericoPorNombre || esNumericoPorDatos) {
                camposNumericos.add(campo);
                log.debug("✅ Campo numérico detectado: '{}' (por nombre: {}, por datos: {})",
                        campo, esNumericoPorNombre, esNumericoPorDatos);
            } else {
                log.trace("❌ Campo NO numérico: '{}' (por nombre: {}, por datos: {})",
                        campo, esNumericoPorNombre, esNumericoPorDatos);
            }
        }

        log.info("🔢 Campos numéricos detectados: {}", camposNumericos);
        return camposNumericos;
    }

    /**
     * 📍 Agrega campos de ubicación automáticamente si son necesarios
     */
    private void agregarCamposUbicacionSiNecesario(List<String> camposActuales,
                                                   Set<String> camposDisponibles) {

        // Lista de campos de ubicación en orden de prioridad
        String[] camposUbicacion = {"provincia", "municipio", "lugar", "contexto"};

        for (String campo : camposUbicacion) {
            if (!camposActuales.contains(campo) && camposDisponibles.contains(campo)) {
                camposActuales.add(campo);
                log.debug("Campo de ubicación agregado automáticamente: {}", campo);

                // Limitar a máximo 2 campos de ubicación adicionales
                if (camposActuales.stream().mapToLong(c -> esGeografico(c) ? 1 : 0).sum() >= 3) {
                    break;
                }
            }
        }
    }

    /**
     * 🧮 NUEVO: Consolidación respetando el orden de campos
     */
    private List<Map<String, Object>> consolidarPorCamposOrdenados(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("Sin campos de agrupación válidos");
            return datos;
        }

        log.info("🧮 Procesando consolidación ordenada: {} registros → agrupación por {}",
                datos.size(), camposAgrupacion);

        // Usar LinkedHashMap para mantener orden de inserción
        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();
        int registrosProcesados = 0;

        for (Map<String, Object> registro : datos) {
            // Crear clave respetando el ORDEN especificado por el usuario
            String claveGrupo = crearClaveGrupoOrdenada(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> inicializarNuevoGrupoOrdenado(registro, camposAgrupacion, camposNumericos));

            acumularCamposNumericos(grupo, registro, camposNumericos);

            registrosProcesados++;

            // Log de progreso cada 10,000 registros
            if (registrosProcesados % 10000 == 0) {
                log.debug("Procesados {} registros, {} grupos creados",
                        registrosProcesados, grupos.size());
            }
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());

        log.info("✅ Consolidación ordenada completada: {} registros → {} grupos únicos",
                datos.size(), resultado.size());

        return resultado;
    }

    /**
     * 🔑 Crea clave de grupo manteniendo el orden especificado
     */
    private String crearClaveGrupoOrdenada(Map<String, Object> registro,
                                           List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> {
                    Object valor = registro.get(campo);
                    return valor != null ? valor.toString() : "NULL";
                })
                .collect(Collectors.joining("||")); // Separador más distintivo
    }

    /**
     * 🏗️ Inicializa grupo manteniendo el orden de campos
     */
    private Map<String, Object> inicializarNuevoGrupoOrdenado(
            Map<String, Object> registro,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        // Usar LinkedHashMap para mantener orden
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Agregar campos de agrupación EN EL ORDEN ESPECIFICADO
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Inicializar campos numéricos en 0
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    // =============== MÉTODOS DE CLASIFICACIÓN MEJORADOS ===============

    private boolean esGeografico(String campo) {
        return org.transito_seguro.utils.SqlFieldDetector.esGeografico(campo);
    }

    private boolean esCategorico(String campo) {
        return org.transito_seguro.utils.SqlFieldDetector.esCategorico(campo);
    }

    // =============== MÉTODOS EXISTENTES (MANTENIDOS PARA COMPATIBILIDAD) ===============

    /**
     * Valida si una consulta puede ser consolidada.
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
     */
    public boolean puedeSerConsolidada(String nombreQuery) {
        try {
            AnalisisConsolidacion analisis =
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
     */
    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            log.debug("Generando respuesta vacía para formato: {}", formato);
            return Collections.emptyList();
        }

        log.debug("Generando respuesta consolidada: {} registros, formato: {}", datos.size(), formato);
        return datos;
    }

    // =============== MÉTODOS INTERNOS EXISTENTES (SIMPLIFICADOS) ===============

    private AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        try {
            return queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
        } catch (Exception e) {
            log.error("Error obteniendo análisis para query '{}': {}", nombreQuery, e.getMessage());
            return crearAnalisisVacio();
        }
    }

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

    private List<Map<String, Object>> aplicarLimites(List<Map<String, Object>> datos,
                                                     ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        log.debug("Aplicando límites - Offset: {}, Límite: {}, Datos disponibles: {}",
                offset, limite, datos.size());

        if (offset > 0 && offset < datos.size()) {
            datos = datos.subList(offset, datos.size());
        }

        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        log.debug("Límites aplicados - Registros finales: {}", datos.size());
        return datos;
    }

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

    private Long obtenerValorLong(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

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

    // =============== MÉTODOS LEGACY PARA COMPATIBILIDAD ===============

    private List<String> determinarCamposAgrupacionLegacy(ParametrosFiltrosDTO filtros,
                                                          AnalisisConsolidacion analisis) {
        List<String> preferenciasUsuario = filtros.getConsolidacionSeguro();

        if (!preferenciasUsuario.isEmpty()) {
            return new ArrayList<>(preferenciasUsuario);
        }

        List<String> campos = new ArrayList<>(analisis.getCamposUbicacion());
        if (!campos.contains("provincia")) {
            campos.add(0, "provincia");
        }
        return campos;
    }

    private List<String> determinarCamposNumericosLegacy(List<Map<String, Object>> datos,
                                                         AnalisisConsolidacion analisis) {
        if (!analisis.getCamposNumericos().isEmpty()) {
            return analisis.getCamposNumericos();
        }
        return detectarCamposNumericosDinamicos(datos);
    }

    private List<Map<String, Object>> consolidarPorCamposTradicional(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        return consolidarPorCamposOrdenados(datos, camposAgrupacion, camposNumericos);
    }
}