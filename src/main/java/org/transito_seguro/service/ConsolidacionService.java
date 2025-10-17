package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.enums.PeriodoTemporal;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.utils.NormalizadorProvincias;

import java.util.*;
import java.util.stream.Collectors;

import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;

/**
 * Servicio especializado en consolidación automática de datos multi-provincia.
 *
 * CARACTERÍSTICAS PRINCIPALES:
 * - Consolidación jerárquica inteligente basada en campos del usuario
 * - Agrupación dinámica que respeta el orden de campos solicitados
 * - Soporte para consolidación geográfica automática
 * - Detección automática de campos numéricos en tiempo real
 * - Consolidación temporal por día, mes o año
 *
 * @author Sistema Tránsito Seguro
 * @version 2.0 - Corregida consolidación por campos únicos
 */
@Slf4j
@Service
public class ConsolidacionService {

    // =============== DEPENDENCIAS ===============

    @Autowired
    private RepositoryFactory repositoryFactory;

    @Autowired
    private QueryRegistryService queryRegistryService;

    // =============== CONFIGURACIÓN ===============

    /** Mínimo de registros no nulos para considerar un campo como numérico */
    private static final int MIN_MUESTRA_NUMERICA = 3;

    /** Umbral de valores numéricos para clasificar un campo como numérico (80%) */
    private static final double UMBRAL_NUMERICO = 0.8;

    /** Tamaño de muestra para análisis dinámico de campos */
    private static final int TAMAÑO_MUESTRA = 50;

    // =============== ENUMS ===============

    /**
     * Estrategias de consolidación disponibles
     */
    public enum EstrategiaConsolidacion {
        JERARQUICA_USUARIO,      // Usa exactamente los campos solicitados por el usuario
        GEOGRAFICA_AUTOMATICA,   // Agrupa por ubicación geográfica (provincia, municipio, etc.)
        INTELIGENTE_MIXTA        // Combina campos categóricos y geográficos
    }

    // =============== API PRINCIPAL ===============

    /**
     * Consolida datos de múltiples provincias según los parámetros solicitados.
     *
     * Proceso:
     * 1. Determina estrategia de consolidación según campos solicitados
     * 2. Recopila datos de todas las provincias
     * 3. Normaliza nombres de provincias
     * 4. Aplica consolidación temporal si se solicita
     * 5. Aplica consolidación por campos
     * 6. Retorna resultados con límites aplicados
     *
     * @param repositories Lista de repositorios por provincia
     * @param nombreQuery Nombre de la query a ejecutar
     * @param filtros Parámetros de filtrado y consolidación
     * @return Lista de registros consolidados
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("=== CONSOLIDACIÓN INICIADA ===");
        log.info("Provincias: {}, Query: {}, Campos solicitados: {}",
                repositories.size(), nombreQuery, filtros.getConsolidacionSeguro());

        // 1. Determinar estrategia de consolidación
        EstrategiaConsolidacion estrategia = determinarEstrategia(filtros);
        log.info("Estrategia seleccionada: {}", estrategia);

        // 2. Obtener análisis de consolidación
        AnalisisConsolidacion analisis = obtenerAnalisisConsolidacion(nombreQuery);

        // 3. Recopilar datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);
        if (todosLosDatos.isEmpty()) {
            log.info("No se encontraron datos para consolidar");
            return Collections.emptyList();
        }

        // 4. Normalizar provincias
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

        // 5. Detectar y aplicar consolidación temporal si se solicita
        PeriodoTemporal periodoTemporal = detectarPeriodoTemporal(filtros);

        List<Map<String, Object>> datosConsolidados;

        if (periodoTemporal != null) {
            log.info("📅 Consolidación TEMPORAL detectada: {}", periodoTemporal.getDescripcion());
            datosConsolidados = aplicarConsolidacionTemporal(
                    todosLosDatos, periodoTemporal, filtros, analisis);
        } else {
            datosConsolidados = aplicarEstrategiaConsolidacion(
                    todosLosDatos, filtros, analisis, estrategia);
        }

        // 6. Aplicar límites finales
        List<Map<String, Object>> resultado = aplicarLimites(datosConsolidados, filtros);

        log.info("=== CONSOLIDACIÓN COMPLETADA: {} registros finales ===", resultado.size());
        return resultado;
    }

    // =============== ESTRATEGIAS DE CONSOLIDACIÓN ===============

    /**
     * Determina qué estrategia de consolidación usar según los campos solicitados.
     *
     * Lógica:
     * - Sin campos → GEOGRAFICA_AUTOMATICA (agrupa por provincia, municipio)
     * - Con campos mixtos → INTELIGENTE_MIXTA (combina categóricos y geográficos)
     * - Con cualquier campo(s) del usuario → JERARQUICA_USUARIO (usa exactamente esos campos)
     *
     * @param filtros Parámetros de filtrado con campos de consolidación
     * @return Estrategia a aplicar
     */
    private EstrategiaConsolidacion determinarEstrategia(ParametrosFiltrosDTO filtros) {
        List<String> camposUsuario = filtros.getConsolidacionSeguro();

        // CASO 1: Sin campos especificados → modo automático
        if (camposUsuario.isEmpty()) {
            log.info("🤖 Sin campos especificados → AUTOMÁTICO (provincia + municipio)");
            return EstrategiaConsolidacion.GEOGRAFICA_AUTOMATICA;
        }

        log.info("👤 Usuario especificó {} campo(s): {}",
                camposUsuario.size(), camposUsuario);

        // Clasificar tipos de campos
        boolean tieneGeograficos = camposUsuario.stream()
                .anyMatch(this::esGeografico);

        boolean tieneCategoricos = camposUsuario.stream()
                .anyMatch(this::esCategorico);

        // CASO 2: Campos mixtos (geográficos + categóricos)
        if (tieneGeograficos && tieneCategoricos) {
            log.info("🎯 Campos mixtos detectados → MIXTA");
            return EstrategiaConsolidacion.INTELIGENTE_MIXTA;
        }

        // CASO 3: Cualquier cantidad de campos del usuario (1 o más)
        // ⭐ CORRECCIÓN: Siempre usar JERARQUICA_USUARIO para respetar la solicitud
        log.info("✅ Usando campos del usuario → JERÁRQUICA");
        return EstrategiaConsolidacion.JERARQUICA_USUARIO;
    }

    /**
     * Aplica la estrategia de consolidación determinada.
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
     * ESTRATEGIA JERÁRQUICA USUARIO
     *
     * Usa EXACTAMENTE los campos solicitados por el usuario, sin agregar campos adicionales.
     *
     * Ejemplo:
     * - Usuario solicita: ["descripcion"]
     * - Resultado: Agrupa solo por "descripcion" (suma todas las provincias)
     *
     * @param datos Datos a consolidar
     * @param filtros Filtros con campos solicitados
     * @param analisis Análisis de la query
     * @return Datos consolidados por los campos del usuario
     */
    private List<Map<String, Object>> aplicarConsolidacionJerarquicaUsuario(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros,
            AnalisisConsolidacion analisis) {

        log.info("🏗️ Aplicando CONSOLIDACIÓN JERÁRQUICA USUARIO");

        List<String> camposUsuario = new ArrayList<>(filtros.getConsolidacionSeguro());

        // Validar que los campos solicitados existen en los datos
        Set<String> camposDisponibles = datos.isEmpty() ?
                Collections.emptySet() : datos.get(0).keySet();

        List<String> camposValidos = camposUsuario.stream()
                .filter(camposDisponibles::contains)
                .collect(Collectors.toList());

        if (camposValidos.isEmpty()) {
            log.warn("Ningún campo solicitado está disponible en los datos");
            return aplicarConsolidacionGeograficaAutomatica(datos, analisis);
        }

        // ⭐ CORRECCIÓN CRÍTICA: Usar EXACTAMENTE los campos del usuario
        // NO agregar campos adicionales automáticamente
        List<String> camposFinales = new ArrayList<>(camposValidos);

        log.info("🎯 Consolidación EXACTA por: {}", camposFinales);
        log.info("   → Sin campos automáticos adicionales");

        // Detectar campos numéricos dinámicamente
        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("📊 Configuración final:");
        log.info("   🔑 Agrupación: {}", camposFinales);
        log.info("   🔢 Numéricos: {}", camposNumericos);

        // Aplicar consolidación
        return consolidarPorCamposOrdenados(datos, camposFinales, camposNumericos);
    }

    /**
     * ESTRATEGIA GEOGRÁFICA AUTOMÁTICA
     *
     * Se ejecuta cuando el usuario NO especificó campos.
     * Agrupa automáticamente por ubicación geográfica (provincia, municipio, etc.)
     *
     * @param datos Datos a consolidar
     * @param analisis Análisis de la query
     * @return Datos consolidados por geografía
     */
    private List<Map<String, Object>> aplicarConsolidacionGeograficaAutomatica(
            List<Map<String, Object>> datos,
            AnalisisConsolidacion analisis) {

        log.info("🌍 Aplicando CONSOLIDACIÓN GEOGRÁFICA AUTOMÁTICA");
        log.info("   ℹ️ Usuario NO especificó campos → usando geografía por defecto");

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

        // Asegurar que siempre haya al menos "provincia" en modo automático
        if (!camposAgrupacion.contains("provincia") && camposDisponibles.contains("provincia")) {
            camposAgrupacion.add(0, "provincia");
        }

        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Consolidación geográfica automática:");
        log.info("   🔑 Agrupación: {}", camposAgrupacion);
        log.info("   🔢 Numéricos: {}", camposNumericos);

        return consolidarPorCamposOrdenados(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * ESTRATEGIA INTELIGENTE MIXTA
     *
     * Combina campos categóricos y geográficos del usuario,
     * agregando campos de ubicación complementarios si es necesario.
     *
     * @param datos Datos a consolidar
     * @param filtros Filtros con campos solicitados
     * @param analisis Análisis de la query
     * @return Datos consolidados de forma mixta
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
        camposFinales.addAll(camposCategoricos);  // Prioridad a campos categóricos
        camposFinales.addAll(camposGeograficos);  // Luego geográficos

        // Agregar campos de ubicación adicionales si no están
        agregarCamposUbicacionSiNecesario(camposFinales, camposDisponibles);

        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Consolidación mixta:");
        log.info("   🔑 Agrupación: {}", camposFinales);
        log.info("   🔢 Numéricos: {}", camposNumericos);

        return consolidarPorCamposOrdenados(datos, camposFinales, camposNumericos);
    }

    // =============== CONSOLIDACIÓN TEMPORAL ===============

    /**
     * Detecta si se solicita consolidación temporal (día, mes, año).
     *
     * @param filtros Filtros con campos de consolidación
     * @return Período temporal detectado o null
     */
    private PeriodoTemporal detectarPeriodoTemporal(ParametrosFiltrosDTO filtros) {
        List<String> camposUsuario = filtros.getConsolidacionSeguro();

        for (String campo : camposUsuario) {
            String campoNormalizado = campo.toLowerCase().trim();

            // Detectar MES
            if (campoNormalizado.equals("mes") ||
                    campoNormalizado.equals("mes_anio") ||
                    campoNormalizado.contains("mensual")) {
                return PeriodoTemporal.MES;
            }

            // Detectar AÑO
            if (campoNormalizado.equals("anio") ||
                    campoNormalizado.equals("año") ||
                    campoNormalizado.equals("fecha_anio") ||
                    campoNormalizado.contains("anual")) {
                return PeriodoTemporal.ANIO;
            }

            // Detectar DÍA
            if (campoNormalizado.equals("dia") ||
                    campoNormalizado.equals("día") ||
                    campoNormalizado.equals("diario")) {
                return PeriodoTemporal.DIA;
            }
        }

        return null;
    }

    /**
     * Aplica consolidación temporal según el período solicitado.
     *
     * Proceso:
     * 1. Preprocesa datos agregando campos temporales derivados (mes_anio, fecha_anio)
     * 2. Construye campos de agrupación temporal
     * 3. Consolida datos
     * 4. Ordena por período (más reciente primero)
     *
     * @param datos Datos a consolidar
     * @param periodo Período temporal (DIA, MES, ANIO)
     * @param filtros Filtros con configuración
     * @param analisis Análisis de la query
     * @return Datos consolidados temporalmente
     */
    private List<Map<String, Object>> aplicarConsolidacionTemporal(
            List<Map<String, Object>> datos,
            PeriodoTemporal periodo,
            ParametrosFiltrosDTO filtros,
            AnalisisConsolidacion analisis) {

        log.info("📅 Iniciando consolidación temporal por {}", periodo.getDescripcion());

        // Preprocesar datos agregando campos temporales
        datos = preprocesarCamposTemporales(datos, periodo);

        // Construir lista de campos de agrupación
        List<String> camposAgrupacion = construirCamposAgrupacionTemporal(
                datos, periodo, filtros);

        // Detectar campos numéricos
        List<String> camposNumericos = detectarCamposNumericosDinamicos(datos);

        log.info("Configuración temporal:");
        log.info("   🔑 Agrupación: {}", camposAgrupacion);
        log.info("   🔢 Numéricos: {}", camposNumericos);

        // Consolidar
        List<Map<String, Object>> resultado = consolidarPorCamposOrdenados(
                datos, camposAgrupacion, camposNumericos);

        // Ordenar por período (más reciente primero)
        resultado = ordenarPorPeriodoTemporal(resultado, camposAgrupacion.get(0));

        log.info("✅ Consolidación temporal completada: {} períodos únicos", resultado.size());

        return resultado;
    }

    /**
     * Construye los campos de agrupación para consolidación temporal.
     */
    private List<String> construirCamposAgrupacionTemporal(
            List<Map<String, Object>> datos,
            PeriodoTemporal periodo,
            ParametrosFiltrosDTO filtros) {

        List<String> camposAgrupacion = new ArrayList<>();
        Set<String> camposDisponibles = datos.isEmpty() ?
                Collections.emptySet() : datos.get(0).keySet();

        // Agregar campo temporal según el período
        switch (periodo) {
            case MES:
                if (camposDisponibles.contains("mes_anio")) {
                    camposAgrupacion.add("mes_anio");
                }
                break;

            case ANIO:
                if (camposDisponibles.contains("fecha_anio")) {
                    camposAgrupacion.add("fecha_anio");
                }
                break;

            case DIA:
                Set<String> columnasFecha = detectarColumnasFecha(camposDisponibles);
                if (!columnasFecha.isEmpty()) {
                    camposAgrupacion.add(columnasFecha.iterator().next());
                }
                break;
        }

        // Agregar otros campos solicitados por el usuario (excluir indicadores temporales)
        List<String> camposUsuario = filtros.getConsolidacionSeguro();
        for (String campo : camposUsuario) {
            String campoNormalizado = campo.toLowerCase().trim();

            // Saltar indicadores temporales ya procesados
            if (campoNormalizado.matches("(mes|anio|año|dia|día|mensual|anual|diario)")) {
                continue;
            }

            // Agregar si existe y no está duplicado
            if (camposDisponibles.contains(campo) && !camposAgrupacion.contains(campo)) {
                camposAgrupacion.add(campo);
            }
        }

        // Si solo tiene el campo temporal, agregar ubicación geográfica por defecto
        if (camposAgrupacion.size() == 1) {
            agregarCamposUbicacionSiNecesario(camposAgrupacion, camposDisponibles);
        }

        return camposAgrupacion;
    }

    /**
     * Preprocesa datos agregando campos temporales derivados (mes_anio, fecha_anio).
     */
    private List<Map<String, Object>> preprocesarCamposTemporales(
            List<Map<String, Object>> datos,
            PeriodoTemporal periodo) {

        if (datos.isEmpty()) {
            return datos;
        }

        log.info("🔧 Preprocesando {} registros para período: {}",
                datos.size(), periodo.getDescripcion());

        Set<String> columnasFecha = detectarColumnasFecha(datos.get(0).keySet());

        if (columnasFecha.isEmpty()) {
            log.warn("No se encontraron columnas de fecha para preprocesar");
            return datos;
        }

        log.debug("Columnas de fecha detectadas: {}", columnasFecha);

        // Procesar cada registro
        int procesados = 0;
        for (Map<String, Object> registro : datos) {
            for (String columnaFecha : columnasFecha) {
                Object valorFecha = registro.get(columnaFecha);

                if (valorFecha != null) {
                    procesarCampoFecha(registro, valorFecha, periodo);
                    procesados++;
                    break;
                }
            }
        }

        log.info("✅ Campos temporales agregados a {} registros", procesados);
        return datos;
    }

    /**
     * Procesa un campo de fecha y agrega campos derivados según el período.
     */
    private void procesarCampoFecha(Map<String, Object> registro,
                                    Object valorFecha,
                                    PeriodoTemporal periodo) {
        switch (periodo) {
            case MES:
                String mesAnio = extraerMesAnio(valorFecha);
                if (mesAnio != null) {
                    registro.put("mes_anio", mesAnio);
                    registro.put("fecha_mes", extraerMes(valorFecha));
                }
                break;

            case ANIO:
                Integer anio = extraerAnio(valorFecha);
                if (anio != null) {
                    registro.put("fecha_anio", anio);
                }
                break;

            case DIA:
                // La fecha original ya existe
                break;
        }
    }

    /**
     * Detecta columnas que contienen fechas.
     */
    private Set<String> detectarColumnasFecha(Set<String> columnas) {
        return columnas.stream()
                .filter(col -> {
                    String colLower = col.toLowerCase();
                    return colLower.contains("fecha") ||
                            colLower.contains("date") ||
                            colLower.endsWith("_at") ||
                            colLower.contains("timestamp");
                })
                .collect(Collectors.toSet());
    }

    /**
     * Extrae mes-año en formato "YYYY-MM".
     */
    private String extraerMesAnio(Object fecha) {
        try {
            if (fecha instanceof java.sql.Date ||
                    fecha instanceof java.sql.Timestamp ||
                    fecha instanceof java.util.Date) {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM");
                return sdf.format((java.util.Date) fecha);
            }

            if (fecha instanceof String) {
                String strFecha = fecha.toString().trim();

                // Formato ISO: "2025-01-15"
                if (strFecha.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                    return strFecha.substring(0, 7);
                }

                // Formato español: "15/01/2025"
                if (strFecha.matches("\\d{2}/\\d{2}/\\d{4}.*")) {
                    String anio = strFecha.substring(6, 10);
                    String mes = strFecha.substring(3, 5);
                    return anio + "-" + mes;
                }
            }
        } catch (Exception e) {
            log.warn("Error extrayendo mes-año de: {}", fecha, e);
        }
        return null;
    }

    /**
     * Extrae mes (1-12).
     */
    private Integer extraerMes(Object fecha) {
        try {
            if (fecha instanceof java.sql.Date ||
                    fecha instanceof java.sql.Timestamp ||
                    fecha instanceof java.util.Date) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime((java.util.Date) fecha);
                return cal.get(java.util.Calendar.MONTH) + 1;
            }

            if (fecha instanceof String) {
                String strFecha = fecha.toString().trim();

                // ISO: "2025-01-15"
                if (strFecha.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                    return Integer.parseInt(strFecha.substring(5, 7));
                }

                // Español: "15/01/2025"
                if (strFecha.matches("\\d{2}/\\d{2}/\\d{4}.*")) {
                    return Integer.parseInt(strFecha.substring(3, 5));
                }
            }
        } catch (Exception e) {
            log.warn("Error extrayendo mes de: {}", fecha, e);
        }
        return null;
    }

    /**
     * Extrae año.
     */
    private Integer extraerAnio(Object fecha) {
        try {
            if (fecha instanceof java.sql.Date ||
                    fecha instanceof java.sql.Timestamp ||
                    fecha instanceof java.util.Date) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime((java.util.Date) fecha);
                return cal.get(java.util.Calendar.YEAR);
            }

            if (fecha instanceof String) {
                String strFecha = fecha.toString().trim();

                // ISO: "2025-01-15"
                if (strFecha.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                    return Integer.parseInt(strFecha.substring(0, 4));
                }

                // Español: "15/01/2025"
                if (strFecha.matches("\\d{2}/\\d{2}/\\d{4}.*")) {
                    return Integer.parseInt(strFecha.substring(6, 10));
                }
            }
        } catch (Exception e) {
            log.warn("Error extrayendo año de: {}", fecha, e);
        }
        return null;
    }

    /**
     * Ordena resultados por período temporal (más reciente primero).
     */
    private List<Map<String, Object>> ordenarPorPeriodoTemporal(
            List<Map<String, Object>> datos,
            String campoTemporal) {

        datos.sort((a, b) -> {
            Object valorA = a.get(campoTemporal);
            Object valorB = b.get(campoTemporal);

            if (valorA == null && valorB == null) return 0;
            if (valorA == null) return 1;
            if (valorB == null) return -1;

            // Orden DESCENDENTE (más reciente primero)
            return valorB.toString().compareTo(valorA.toString());
        });

        return datos;
    }

    // =============== DETECCIÓN DE CAMPOS ===============

    /**
     * Detecta automáticamente campos numéricos en los datos.
     * Excluye campos de identificación (id, codigo, dni, etc.)
     *
     * @param datos Datos a analizar
     * @return Lista de campos numéricos detectados
     */
    private List<String> detectarCamposNumericosDinamicos(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> todosLosCampos = datos.get(0).keySet();
        List<String> camposNumericos = new ArrayList<>();

        // Campos que nunca deben sumarse
        Set<String> camposExcluidos = new HashSet<>(Arrays.asList(
                "id", "ID", "Id",
                "codigo", "legajo", "dni", "cuit", "cuil",
                "numero_infraccion", "acta", "boleta",
                "id_infraccion", "infraccion_id"
        ));

        log.info("🔍 Analizando campos disponibles: {}", todosLosCampos);

        for (String campo : todosLosCampos) {
            // Saltar campos de identificación
            if (camposExcluidos.contains(campo.toLowerCase())) {
                log.info("⏭️ Campo '{}' excluido (identificador)", campo);
                continue;
            }

            boolean esNumericoPorNombre = org.transito_seguro.utils.SqlFieldDetector.esNumericoPorNombre(campo);
            boolean esNumericoPorDatos = esNumericoEnDatos(campo, datos);

            log.debug("Campo '{}' - Por nombre: {}, Por datos: {}",
                    campo, esNumericoPorNombre, esNumericoPorDatos);

            if (esNumericoPorNombre || esNumericoPorDatos) {
                camposNumericos.add(campo);
                log.info("✅ Campo numérico detectado: '{}'", campo);
            }
        }

        log.info("🔢 Campos numéricos finales: {}", camposNumericos);
        return camposNumericos;
    }

    /**
     * Verifica si un campo es numérico analizando sus valores en los datos.
     *
     * @param campo Nombre del campo
     * @param datos Datos a analizar
     * @return true si el campo es numérico
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

                    if (i < 5) {
                        log.trace("  Muestra[{}] campo '{}' = {} (tipo: {})",
                                i, campo, valor, valor.getClass().getSimpleName());
                    }
                }
            }
        }

        if (contadorNoNulo < MIN_MUESTRA_NUMERICA) {
            log.debug("  ⚠️ Campo '{}' tiene pocos valores no nulos ({} < {})",
                    campo, contadorNoNulo, MIN_MUESTRA_NUMERICA);
            return false;
        }

        double porcentajeNumerico = (double) contadorNumerico / contadorNoNulo;
        boolean esNumerico = porcentajeNumerico > UMBRAL_NUMERICO;

        log.debug("  📊 Análisis campo '{}': {}/{} valores numéricos ({}%) → {}",
                campo, contadorNumerico, contadorNoNulo,
                String.format("%.1f", porcentajeNumerico * 100),
                esNumerico ? "NUMÉRICO" : "NO NUMÉRICO");

        return esNumerico;
    }

    /**
     * Verifica si un valor individual es numérico.
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

    /**
     * Verifica si un campo es geográfico.
     */
    private boolean esGeografico(String campo) {
        return org.transito_seguro.utils.SqlFieldDetector.esGeografico(campo);
    }

    /**
     * Verifica si un campo es categórico.
     */
    private boolean esCategorico(String campo) {
        return org.transito_seguro.utils.SqlFieldDetector.esCategorico(campo);
    }

    /**
     * Verifica si un campo es categórico puro (no geográfico).
     */
    private boolean esCategoricoPuro(String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return false;
        }

        String campoNormalizado = campo.toLowerCase().trim();

        Set<String> camposCategoricos = new HashSet<>(Arrays.asList(
                "descripcion", "descripción",
                "estado", "status",
                "tipo", "type",
                "categoria", "category",
                "clase", "class",
                "articulo", "artículo",
                "infraccion", "infracción",
                "gravedad",
                "tipo_vehiculo", "tipo_vehículo"
        ));

        if (esGeografico(campo)) {
            return false;
        }

        return camposCategoricos.contains(campoNormalizado);
    }

    /**
     * Agrega campos de ubicación complementarios si son necesarios.
     * Se usa en estrategia MIXTA para completar la jerarquía geográfica.
     *
     * @param camposActuales Lista actual de campos
     * @param camposDisponibles Campos disponibles en los datos
     */
    private void agregarCamposUbicacionSiNecesario(List<String> camposActuales,
                                                   Set<String> camposDisponibles) {

        // Si ya tiene suficientes campos geográficos, no agregar más
        long camposGeograficosActuales = camposActuales.stream()
                .filter(this::esGeografico)
                .count();

        if (camposGeograficosActuales >= 2) {
            log.debug("Ya tiene {} campos geográficos, no se agregarán más",
                    camposGeograficosActuales);
            return;
        }

        String[] camposUbicacion = {"provincia", "municipio", "lugar", "contexto"};

        int camposAgregados = 0;
        int maxCamposAgregar = 2 - (int) camposGeograficosActuales;

        for (String campo : camposUbicacion) {
            if (!camposActuales.contains(campo) && camposDisponibles.contains(campo)) {
                camposActuales.add(campo);
                camposAgregados++;
                log.debug("Campo de ubicación agregado automáticamente: {}", campo);

                if (camposAgregados >= maxCamposAgregar) {
                    break;
                }
            }
        }

        if (camposAgregados > 0) {
            log.info("✅ Agregados {} campos geográficos complementarios", camposAgregados);
        }
    }

    // =============== CONSOLIDACIÓN CORE ===============

    /**
     * Consolida datos respetando el orden de campos especificado.
     *
     * Proceso:
     * 1. Agrupa registros por los campos de agrupación
     * 2. Suma los valores de los campos numéricos
     * 3. Mantiene el primer valor de los campos de agrupación
     *
     * @param datos Datos a consolidar
     * @param camposAgrupacion Campos por los que agrupar
     * @param camposNumericos Campos a sumar
     * @return Datos consolidados
     */
    private List<Map<String, Object>> consolidarPorCamposOrdenados(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        if (camposAgrupacion.isEmpty()) {
            log.warn("Sin campos de agrupación válidos");
            return datos;
        }

        log.info("🧮 ==================== INICIO CONSOLIDACIÓN ====================");
        log.info("   📊 Total registros de entrada: {}", datos.size());
        log.info("   🔑 Campos de agrupación: {}", camposAgrupacion);
        log.info("   🔢 Campos numéricos a sumar: {}", camposNumericos);

        if (!datos.isEmpty()) {
            log.info("   📋 Campos disponibles en datos: {}", datos.get(0).keySet());
        }

        // Usar LinkedHashMap para mantener orden de inserción
        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();
        int registrosProcesados = 0;

        for (Map<String, Object> registro : datos) {
            // Crear clave respetando el ORDEN especificado
            String claveGrupo = crearClaveGrupoOrdenada(registro, camposAgrupacion);

            // Si es el primer registro del grupo, inicializar
            if (!grupos.containsKey(claveGrupo)) {
                Map<String, Object> nuevoGrupo = inicializarNuevoGrupoOrdenado(
                        registro, camposAgrupacion, camposNumericos);
                grupos.put(claveGrupo, nuevoGrupo);

                // Log de los primeros 3 grupos
                if (grupos.size() <= 3) {
                    log.info("   📦 Grupo #{} creado:", grupos.size());
                    log.info("      🔑 Clave: {}", claveGrupo);
                    log.info("      📄 Contenido: {}", nuevoGrupo);
                }
            } else {
                // Acumular valores numéricos
                Map<String, Object> grupoExistente = grupos.get(claveGrupo);
                acumularCamposNumericos(grupoExistente, registro, camposNumericos);
            }

            registrosProcesados++;

            // Log de progreso cada 10,000 registros
            if (registrosProcesados % 10000 == 0) {
                log.debug("   ⏳ Procesados {} registros, {} grupos creados",
                        registrosProcesados, grupos.size());
            }
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());

        log.info("✅ ==================== FIN CONSOLIDACIÓN ====================");
        log.info("   📥 Registros originales: {}", datos.size());
        log.info("   📤 Grupos consolidados: {}", resultado.size());
        log.info("   📉 Factor de reducción: {}x",
                String.format("%.2f", datos.size() > 0 ? (double) datos.size() / resultado.size() : 0));

        if (!resultado.isEmpty()) {
            log.info("   📋 Campos en resultado final: {}", resultado.get(0).keySet());
            if (resultado.size() <= 5) {
                log.info("   📊 Muestra de resultados:");
                for (int i = 0; i < Math.min(3, resultado.size()); i++) {
                    log.info("      Resultado #{}: {}", i + 1, resultado.get(i));
                }
            }
        }
        log.info("================================================================");

        return resultado;
    }

    /**
     * Crea una clave única para agrupar registros.
     *
     * @param registro Registro a procesar
     * @param camposAgrupacion Campos que forman la clave
     * @return Clave concatenada
     */
    private String crearClaveGrupoOrdenada(Map<String, Object> registro,
                                           List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> {
                    Object valor = registro.get(campo);
                    return valor != null ? valor.toString() : "NULL";
                })
                .collect(Collectors.joining("||"));
    }

    /**
     * Inicializa un nuevo grupo con los campos de agrupación y numéricos.
     *
     * @param registro Primer registro del grupo
     * @param camposAgrupacion Campos de agrupación
     * @param camposNumericos Campos numéricos
     * @return Nuevo grupo inicializado
     */
    private Map<String, Object> inicializarNuevoGrupoOrdenado(
            Map<String, Object> registro,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        Map<String, Object> grupo = new LinkedHashMap<>();

        // Agregar campos de agrupación
        for (String campo : camposAgrupacion) {
            Object valor = registro.get(campo);
            grupo.put(campo, valor);
            log.trace("   📋 Campo agrupación '{}' = {}", campo, valor);
        }

        // Inicializar campos numéricos con el valor del registro actual
        for (String campo : camposNumericos) {
            Object valorOriginal = registro.get(campo);
            Long valorInicial = convertirALong(valorOriginal);

            grupo.put(campo, valorInicial != null ? valorInicial : 0L);

            log.trace("   🔢 Campo numérico '{}': {} -> {}",
                    campo, valorOriginal, valorInicial);
        }

        return grupo;
    }

    /**
     * Acumula valores numéricos de un registro en un grupo existente.
     *
     * @param grupo Grupo existente
     * @param registro Registro a acumular
     * @param camposNumericos Campos a sumar
     */
    private void acumularCamposNumericos(Map<String, Object> grupo,
                                         Map<String, Object> registro,
                                         List<String> camposNumericos) {

        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);
            Object valorGrupo = grupo.get(campo);

            Long valorNumericoRegistro = convertirALong(valorRegistro);
            Long valorNumericoGrupo = convertirALong(valorGrupo);

            Long nuevoValor = valorNumericoGrupo + valorNumericoRegistro;
            grupo.put(campo, nuevoValor);

            log.trace("Acumulación - Campo: '{}', Grupo: {} + Registro: {} = {}",
                    campo, valorNumericoGrupo, valorNumericoRegistro, nuevoValor);
        }
    }

    /**
     * Convierte un valor a Long para operaciones numéricas.
     *
     * @param valor Valor a convertir
     * @return Valor como Long o 0L si no es convertible
     */
    private Long convertirALong(Object valor) {
        if (valor == null) {
            return 0L;
        }

        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }

        if (valor instanceof String) {
            String str = valor.toString().trim();
            if (str.isEmpty()) {
                return 0L;
            }
            try {
                // Manejar números con decimales
                if (str.matches("^\\d+\\.\\d+$")) {
                    return Long.parseLong(str.split("\\.")[0]);
                }
                return Long.parseLong(str);
            } catch (NumberFormatException e) {
                log.warn("Error convirtiendo '{}' a Long: {}", str, e.getMessage());
                return 0L;
            }
        }

        log.debug("Tipo no soportado para conversión: {} valor: {}",
                valor.getClass().getSimpleName(), valor);
        return 0L;
    }

    // =============== MÉTODOS DE RECOPILACIÓN Y NORMALIZACIÓN ===============

    /**
     * Recopila datos de todos los repositorios (provincias).
     *
     * @param repositories Lista de repositorios
     * @param nombreQuery Nombre de la query
     * @param filtros Filtros a aplicar
     * @return Lista consolidada de datos de todas las provincias
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
                    // Agregar provincia a cada registro
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
     * Normaliza nombres de provincias en todos los registros.
     *
     * @param datos Datos a normalizar
     * @return Datos con provincias normalizadas
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
     * Obtiene el nombre de provincia de un registro.
     *
     * @param registro Registro a procesar
     * @return Nombre de la provincia
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
     * Aplica límites de paginación a los resultados.
     *
     * @param datos Datos a limitar
     * @param filtros Filtros con configuración de límites
     * @return Datos con límites aplicados
     */
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

    /**
     * Obtiene el análisis de consolidación para una query.
     *
     * @param nombreQuery Nombre de la query
     * @return Análisis de consolidación
     */
    private AnalisisConsolidacion obtenerAnalisisConsolidacion(String nombreQuery) {
        try {
            return queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);
        } catch (Exception e) {
            log.error("Error obteniendo análisis para query '{}': {}", nombreQuery, e.getMessage());
            return crearAnalisisVacio();
        }
    }

    // =============== MÉTODOS LEGACY (COMPATIBILIDAD) ===============

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
     * Valida si una query específica puede ser consolidada.
     */
    public boolean puedeSerConsolidada(String nombreQuery) {
        try {
            AnalisisConsolidacion analisis =
                    queryRegistryService.obtenerAnalisisConsolidacion(nombreQuery);

            boolean consolidable = analisis.isEsConsolidable();
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

    /**
     * Determina campos de agrupación según preferencias del usuario (legacy).
     */
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

    /**
     * Determina campos numéricos según análisis o detección dinámica (legacy).
     */
    private List<String> determinarCamposNumericosLegacy(List<Map<String, Object>> datos,
                                                         AnalisisConsolidacion analisis) {
        if (!analisis.getCamposNumericos().isEmpty()) {
            return analisis.getCamposNumericos();
        }
        return detectarCamposNumericosDinamicos(datos);
    }

    /**
     * Consolidación tradicional (legacy).
     */
    private List<Map<String, Object>> consolidarPorCamposTradicional(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        return consolidarPorCamposOrdenados(datos, camposAgrupacion, camposNumericos);
    }
}