package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.model.CampoAnalizado;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;

import java.util.*;
import java.util.regex.Matcher;


import static org.transito_seguro.enums.TipoCampo.determinarTipoCampo;
import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;
import static org.transito_seguro.utils.RegexUtils.*;

@Slf4j
@Component
public class QueryAnalyzer {


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


}