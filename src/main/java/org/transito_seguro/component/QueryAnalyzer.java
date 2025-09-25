package org.transito_seguro.component;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class QueryAnalyzer {

    @Getter
    @AllArgsConstructor
    public static class AnalisisConsolidacion{
        private final List<String> camposAgrupacion; // Para GROUP BY en consolidacion
        private final List<String> camposNumericos; // para SUM por consolidacion
        private final List<String> camposTiempo; // para agrupacion temporal ( fecha inico & fecha fin )
        private final List<String> camposUbicacion; // provincia,contexto,municipio
        private final Map<String, TipoCampo> tipoPorCampo;
        private final boolean esConsolidado; // Si la query puede ser consolidada
    }

    @AllArgsConstructor
    private static class CampoAnalizado {
        final String expresionOriginal;
        final String expresionLimpia;
        final String nombreFinal;
        final TipoCampo tipo;
        final boolean esAgregacion;
        final boolean esCalculado;
    }

    // Metodo auxillar para crear el mapa con categorias
    private static Map<String, TipoCampo> createCategorizedMap(TipoCampo categoria, String... keys) {
        // 1. Recolecta las claves en un mapa mutable estándar.
        Map<String, TipoCampo> mutableMap = Stream.of(keys)
                .collect(Collectors.toMap(
                        key -> key,      // La clave es la propia cadena
                        key -> categoria // El valor es siempre la categoría pasada como argumento
                ));

        // 2. Devuelve una vista inmutable de ese mapa.
        return Collections.unmodifiableMap(mutableMap);
    }


    public static final Map<String, TipoCampo> CAMPOS_UBICACION = createCategorizedMap(
            TipoCampo.UBICACION,
            "provincia", "municipio", "contexto", "lugar", "partido", "localidad"
    );

    public static final Map<String, TipoCampo> CAMPOS_CATEGORIZACION = createCategorizedMap(
            TipoCampo.CATEGORIZACION,
            "tipo_infraccion", "serie_equipo", "descripcion", "estado", "tipo_documento", "vehiculo"
    );

    public static final Map<String, TipoCampo> CAMPOS_TIEMPO = createCategorizedMap(
            TipoCampo.TIEMPO,
            "fecha", "fecha_reporte", "fecha_emision", "fecha_alta", "fecha_titularidad",
            "ultima_modificacion", "fecha_constatacion", "mes_exportacion"
    );

    public static final Map<String, TipoCampo> CAMPOS_NUMERICOS_CONOCIDOS = createCategorizedMap(
            TipoCampo.NUMERICO_SUMA,
            "total", "cantidad", "vehiculos", "motos", "formato_no_valido", "counter",
            "luz_roja", "senda", "total_sin_email", "velocidad_radar_fijo",
            "pre_aprobada", "filtrada_municipio", "sin_datos", "prescrita", "filtrada",
            "aprobada", "pdf_generado", "total_eventos"
    );

    // Patrones regex para análisis SQL
    private static final Pattern SELECT_PATTERN =
            Pattern.compile("SELECT\\s+(.+?)\\s+FROM", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern CAMPO_AS_PATTERN =
            Pattern.compile("(.+?)\\s+AS\\s+[\"']?([^,\"'\\s]+)[\"']?", Pattern.CASE_INSENSITIVE);

    private static final Pattern SUM_PATTERN =
            Pattern.compile("SUM\\s*\\([^)]+\\)", Pattern.CASE_INSENSITIVE);

    private static final Pattern COUNT_PATTERN =
            Pattern.compile("COUNT\\s*\\([^)]+\\)", Pattern.CASE_INSENSITIVE);

    private static final Pattern CASE_WHEN_PATTERN =
            Pattern.compile("CASE\\s+WHEN.+?END", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);



    //============================ ANALISIS DE CONSOLIDACION ================================================//

    public AnalisisConsolidacion analizarParaConsolidacion(String query) {
        log.debug("Analizando query SQL para consolidación dinámica");

        try {


            // 1. Extraer SELECT clause
            String selectClause = extraerSelectClause(query);
            if (selectClause == null) {
                log.warn("No se pudo extraer SELECT de la query");
                return crearAnalisisVacio();
            }


            // 2. Extraer campos individuales del SELECT
            List<CampoAnalizado> campos = parsearCamposSelect(selectClause);

            // 3. Clasificar campos por tipo y propósito
            return clasificarCamposParaConsolidacion(campos, query);

        } catch (Exception e) {
            log.error("Error analizando query para consolidación: {}", e.getMessage(), e);
            return crearAnalisisVacio();
        }
    }

    /**
     * Extrae la cláusula SELECT completa
     */
    private String extraerSelectClause(String query) {
        Matcher matcher = SELECT_PATTERN.matcher(query.trim());
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Parsea campos individuales del SELECT
     */
    private List<CampoAnalizado> parsearCamposSelect(String selectClause) {
        List<CampoAnalizado> campos = new ArrayList<>();

        // Dividir por comas respetando paréntesis y funciones
        List<String> camposRaw = dividirCamposInteligente(selectClause);

        for (String campoRaw : camposRaw) {
            CampoAnalizado campo = analizarCampoIndividual(campoRaw.trim());
            if (campo != null) {
                campos.add(campo);
                log.trace("Campo parseado: {} -> {}", campo.expresionOriginal, campo.nombreFinal);
            }
        }

        log.debug("Parseados {} campos del SELECT", campos.size());
        return campos;
    }

    /**
     * Analiza un campo individual del SELECT
     */
    private CampoAnalizado analizarCampoIndividual(String expresion) {
        // Detectar si tiene alias (AS)
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        String expresionLimpia;
        String nombreFinal;

        if (aliasMatcher.matches()) {
            expresionLimpia = aliasMatcher.group(1).trim();
            nombreFinal = aliasMatcher.group(2).trim().toLowerCase();
            // Remover comillas del alias
            nombreFinal = nombreFinal.replaceAll("[\"']", "");
        } else {
            expresionLimpia = expresion.trim();
            // Extraer nombre del campo sin prefijo de tabla (x.campo -> campo)
            nombreFinal = expresionLimpia.replaceAll("^[a-zA-Z_]+\\.", "").toLowerCase();
        }

        // Determinar tipo de campo
        TipoCampo tipo = determinarTipoCampo(expresionLimpia, nombreFinal);
        boolean esAgregacion = esExpresionAgregacion(expresionLimpia);
        boolean esCalculado = esExpresionCalculada(expresionLimpia);

        return new CampoAnalizado(expresion, expresionLimpia, nombreFinal, tipo, esAgregacion, esCalculado);
    }

    /**
     * Determina el tipo de campo basado en patrones conocidos del sistema
     */
    private TipoCampo determinarTipoCampo(String expresion, String nombreFinal) {
        // 1. Por nombre conocido (más confiable)
        if (CAMPOS_UBICACION.containsKey(nombreFinal)) {
            return TipoCampo.UBICACION;
        }

        if (CAMPOS_CATEGORIZACION.containsKey(nombreFinal)) {
            return TipoCampo.CATEGORIZACION;
        }

        if (CAMPOS_TIEMPO.containsKey(nombreFinal) || nombreFinal.contains("fecha")) {
            return TipoCampo.TIEMPO;
        }

        if (CAMPOS_NUMERICOS_CONOCIDOS.containsKey(nombreFinal)) {
            return TipoCampo.NUMERICO_SUMA;
        }

        // 2. Por expresión SQL
        if (SUM_PATTERN.matcher(expresion).find()) {
            return TipoCampo.NUMERICO_SUMA;
        }

        if (COUNT_PATTERN.matcher(expresion).find()) {
            return TipoCampo.NUMERICO_COUNT;
        }

        if (CASE_WHEN_PATTERN.matcher(expresion).find()) {
            return TipoCampo.CALCULADO;
        }

        // 3. Por patrones de nombre
        if (nombreFinal.contains("total") || nombreFinal.contains("cantidad") ||
                nombreFinal.contains("counter") || nombreFinal.endsWith("_count")) {
            return TipoCampo.NUMERICO_SUMA;
        }

        if (nombreFinal.contains("dominio") || nombreFinal.contains("documento") ||
                nombreFinal.contains("cuit") || nombreFinal.contains("nro_")) {
            return TipoCampo.IDENTIFICADOR;
        }

        // 4. Default para campos de detalle
        return TipoCampo.DETALLE;
    }

    /**
     * Clasifica campos para estrategia de consolidación
     */
    private AnalisisConsolidacion clasificarCamposParaConsolidacion(List<CampoAnalizado> campos, String query) {
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();
        List<String> camposTiempo = new ArrayList<>();
        List<String> camposUbicacion = new ArrayList<>();
        Map<String, TipoCampo> tipoPorCampo = new HashMap<>();

        for (CampoAnalizado campo : campos) {
            tipoPorCampo.put(campo.nombreFinal, campo.tipo);

            switch (campo.tipo) {
                case UBICACION:
                    camposUbicacion.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case CATEGORIZACION:
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case TIEMPO:
                    camposTiempo.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case NUMERICO_SUMA:
                case NUMERICO_COUNT:
                    camposNumericos.add(campo.nombreFinal);
                    break;

                case CALCULADO:
                    // Los campos calculados pueden ser agrupación dependiendo del contexto
                    if (campo.nombreFinal.contains("enviado") || campo.nombreFinal.contains("hacia")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                case IDENTIFICADOR:
                case DETALLE:
                    // Generalmente no se usan para consolidación, pero pueden ser agrupación en algunos casos
                    if (campo.nombreFinal.equals("serie_equipo") || campo.nombreFinal.equals("lugar")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;
            }
        }

        // Determinar si es consolidable
        boolean esConsolidable = !camposNumericos.isEmpty() && !camposAgrupacion.isEmpty();

        // Si no hay campos de ubicación explícitos, agregar "provincia" como default
        if (camposUbicacion.isEmpty() && esConsolidable) {
            camposUbicacion.add("provincia");
            if (!camposAgrupacion.contains("provincia")) {
                camposAgrupacion.add("provincia");
            }
        }

        log.info("Análisis completado - Consolidable: {}, Agrupación: {}, Numéricos: {}, Ubicación: {}",
                esConsolidable, camposAgrupacion.size(), camposNumericos.size(), camposUbicacion.size());

        return new AnalisisConsolidacion(camposAgrupacion, camposNumericos, camposTiempo,
                camposUbicacion, tipoPorCampo, esConsolidable);
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private List<String> dividirCamposInteligente(String selectClause) {
        List<String> campos = new ArrayList<>();
        StringBuilder campoActual = new StringBuilder();
        int nivelParentesis = 0;
        boolean enComillas = false;
        char tipoComilla = 0;

        for (char c : selectClause.toCharArray()) {
            if (!enComillas && (c == '\'' || c == '"')) {
                enComillas = true;
                tipoComilla = c;
            } else if (enComillas && c == tipoComilla) {
                enComillas = false;
            } else if (!enComillas) {
                if (c == '(') nivelParentesis++;
                else if (c == ')') nivelParentesis--;
                else if (c == ',' && nivelParentesis == 0) {
                    campos.add(campoActual.toString().trim());
                    campoActual = new StringBuilder();
                    continue;
                }
            }
            campoActual.append(c);
        }

        if (campoActual.length() > 0) {
            campos.add(campoActual.toString().trim());
        }

        return campos;
    }

    private boolean esExpresionAgregacion(String expresion) {
        return SUM_PATTERN.matcher(expresion).find() ||
                COUNT_PATTERN.matcher(expresion).find() ||
                expresion.toUpperCase().contains("AVG(") ||
                expresion.toUpperCase().contains("MAX(") ||
                expresion.toUpperCase().contains("MIN(");
    }

    private boolean esExpresionCalculada(String expresion) {
        return CASE_WHEN_PATTERN.matcher(expresion).find() ||
                expresion.contains("COALESCE(") ||
                expresion.contains("TO_CHAR(") ||
                expresion.contains("CONCAT(");
    }

    private AnalisisConsolidacion crearAnalisisVacio() {
        return new AnalisisConsolidacion(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                false
        );
    }

    /**
     * Genera configuración de consolidación automática basada en la query
     */
    public List<String> generarConfiguracionConsolidacion(String query, String estrategia) {
        AnalisisConsolidacion analisis = analizarParaConsolidacion(query);

        if (!analisis.esConsolidado) {
            log.warn("Query no es consolidable, retornando configuración vacía");
            return Collections.emptyList();
        }

        List<String> configuracion = new ArrayList<>();

        switch (estrategia.toLowerCase()) {
            case "provincia":
                configuracion.addAll(analisis.camposUbicacion);
                break;

            case "completa":
                configuracion.addAll(analisis.camposAgrupacion);
                break;

            case "temporal":
                configuracion.addAll(analisis.camposTiempo);
                configuracion.addAll(analisis.camposUbicacion);
                break;

            default:
                // Estrategia inteligente: ubicación + categorización principal
                configuracion.addAll(analisis.camposUbicacion);
                // Agregar máximo 2 campos de categorización más importantes
                analisis.camposAgrupacion.stream()
                        .filter(campo -> !analisis.camposUbicacion.contains(campo))
                        .filter(campo -> !analisis.camposTiempo.contains(campo))
                        .limit(2)
                        .forEach(configuracion::add);
        }

        log.info("Configuración generada para estrategia '{}': {}", estrategia, configuracion);
        return configuracion;
    }

    /**
     * Valida si una query específica puede ser consolidada
     */
    public boolean puedeSerConsolidada(String query) {
        return analizarParaConsolidacion(query).esConsolidado;
    }






}
