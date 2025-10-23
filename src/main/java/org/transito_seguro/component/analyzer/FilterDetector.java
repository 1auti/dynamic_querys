package org.transito_seguro.component.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.model.FiltroMetadata;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Componente especializado en detectar filtros disponibles en queries SQL.
 *
 * Responsabilidades:
 * - Analizar cláusula WHERE
 * - Detectar filtros de fecha, estado, tipo, concesión, etc.
 * - Generar metadata para UI (tipo de control, parámetros, opciones)
 * - Identificar filtros hardcodeados vs dinámicos
 */
@Slf4j
@Component
public class FilterDetector {

    // ========== CLASE INTERNA ==========

    /**
     * Resultado de la detección de un filtro.
     */
    private static class DeteccionFiltro {
        boolean detectado;
        String campoSQL;
        FiltroMetadata metadata;
        boolean esHardcodeado;

        DeteccionFiltro(boolean detectado, String campoSQL, FiltroMetadata metadata, boolean esHardcodeado) {
            this.detectado = detectado;
            this.campoSQL = campoSQL;
            this.metadata = metadata;
            this.esHardcodeado = esHardcodeado;
        }
    }

    @Autowired
    private SqlParser sqlParser;

    /**
     * Analiza una query SQL para detectar qué filtros están disponibles.
     *
     * @param query Query SQL a analizar
     * @return Map con key=nombre_filtro, value=metadata del filtro
     */
    public Map<String, FiltroMetadata> detectarFiltrosDisponibles(String query) {
        log.debug("=== DETECTANDO FILTROS DISPONIBLES EN QUERY ===");

        Map<String, FiltroMetadata> filtrosDisponibles = new LinkedHashMap<>();

        try {
            // Limpiar query y extraer WHERE
            String queryLimpia = sqlParser.limpiarComentariosSQL(query);
            String whereClause = sqlParser.extraerWhereClause(queryLimpia);

            if (whereClause == null || whereClause.trim().isEmpty()) {
                log.debug("Query sin cláusula WHERE - No hay filtros para detectar");
                return filtrosDisponibles;
            }

            log.debug("WHERE extraído: {}", whereClause);

            // Detectar cada tipo de filtro
            detectarFiltroFecha(whereClause, filtrosDisponibles);
            detectarFiltroEstado(whereClause, filtrosDisponibles);
            detectarFiltroTipoInfraccion(whereClause, filtrosDisponibles);
            detectarFiltroExportaSacit(whereClause, filtrosDisponibles);
            detectarFiltroConcesion(whereClause, filtrosDisponibles);

            log.info("✅ Detectados {} filtros disponibles: {}",
                    filtrosDisponibles.size(),
                    filtrosDisponibles.keySet());

        } catch (Exception e) {
            log.error("Error detectando filtros disponibles: {}", e.getMessage(), e);
        }

        return filtrosDisponibles;
    }

    /**
     * Detecta filtros de fecha en la query.
     *
     * Busca patrones como:
     * - fecha BETWEEN ... AND ...
     * - fecha >= ...
     * - fecha = ...
     */
    private void detectarFiltroFecha(
            String whereClause,
            Map<String, FiltroMetadata> filtros) {

        Pattern fechaPattern = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_]*fecha[a-zA-Z_]*)\\s*(>=|>|<|<=|=|BETWEEN)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = fechaPattern.matcher(whereClause);

        if (matcher.find()) {
            String campoSQL = matcher.group(1);
            String nombreCampo = campoSQL.replaceAll("^[a-zA-Z_]+\\.", "");
            String etiqueta = generarEtiquetaLegible(nombreCampo);

            FiltroMetadata filtroFecha = FiltroMetadata.builder()
                    .tipoFiltro("DATE_RANGE")
                    .campoSQL(campoSQL)
                    .etiqueta(etiqueta)
                    .parametros(Arrays.asList("fechaEspecifica", "fechaInicio", "fechaFin"))
                    .tipoDato("DATE")
                    .multiple(false)
                    .obligatorio(false)
                    .build();

            filtros.put("fecha", filtroFecha);
            log.debug("✓ Filtro FECHA detectado: {} → {}", campoSQL, etiqueta);
        }
    }

    /**
     * Detecta filtros de estado.
     *
     * Busca patrones:
     * - id_estado IN (...) → Múltiples valores
     * - id_estado = 340 → Valor hardcodeado
     * - id_estado = ANY(:estadosInfracciones) → Ya dinámico
     */
    private void detectarFiltroEstado(
            String whereClause,
            Map<String, FiltroMetadata> filtros) {

        DeteccionFiltro deteccion = detectarFiltroArray(
                whereClause,
                "id_estado",
                "Estados de Infracción",
                "estadosInfracciones"
        );

        if (deteccion.detectado) {
            filtros.put("estado", deteccion.metadata);

            if (deteccion.esHardcodeado) {
                log.info("💡 SUGERENCIA: Convertir {} a filtro dinámico usando DynamicBuilderQuery",
                        deteccion.campoSQL);
            }
        }
    }

    /**
     * Detecta filtros de tipo de infracción.
     */
    private void detectarFiltroTipoInfraccion(
            String whereClause,
            Map<String, FiltroMetadata> filtros) {

        DeteccionFiltro deteccion = detectarFiltroArray(
                whereClause,
                "id_tipo_infra",
                "Tipos de Infracción",
                "tiposInfracciones"
        );

        if (deteccion.detectado) {
            filtros.put("tipo_infraccion", deteccion.metadata);

            if (deteccion.esHardcodeado) {
                log.info("💡 SUGERENCIA: Convertir {} a filtro dinámico", deteccion.campoSQL);
            }
        }
    }

    /**
     * Detecta filtros booleanos (exporta_sacit).
     */
    private void detectarFiltroExportaSacit(
            String whereClause,
            Map<String, FiltroMetadata> filtros) {

        Pattern sacitPattern = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\.exporta_sacit)\\s*=",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = sacitPattern.matcher(whereClause);

        if (matcher.find()) {
            String campoSQL = matcher.group(1);

            FiltroMetadata filtroSacit = FiltroMetadata.builder()
                    .tipoFiltro("BOOLEAN")
                    .campoSQL(campoSQL)
                    .etiqueta("Exportado a SACIT")
                    .parametros(Arrays.asList("exportadoSacit"))
                    .tipoDato("BOOLEAN")
                    .multiple(false)
                    .obligatorio(false)
                    .opciones(Arrays.asList(
                            FiltroMetadata.OpcionFiltro.builder()
                                    .valor(true)
                                    .etiqueta("Sí (Exportado)")
                                    .build(),
                            FiltroMetadata.OpcionFiltro.builder()
                                    .valor(false)
                                    .etiqueta("No (Sin exportar)")
                                    .build()
                    ))
                    .build();

            filtros.put("exporta_sacit", filtroSacit);
            log.debug("✓ Filtro EXPORTA_SACIT detectado: {}", campoSQL);
        }
    }

    /**
     * Detecta filtros de concesión/municipio.
     */
    private void detectarFiltroConcesion(
            String whereClause,
            Map<String, FiltroMetadata> filtros) {

        DeteccionFiltro deteccion = detectarFiltroArray(
                whereClause,
                "id_concesion",
                "Concesiones / Municipios",
                "concesiones"
        );

        if (deteccion.detectado) {
            filtros.put("concesion", deteccion.metadata);

            if (deteccion.esHardcodeado) {
                log.warn("⚠️ Filtro CONCESION detectado HARDCODEADO: {} - Se recomienda hacer dinámico",
                        deteccion.campoSQL);
            }
        }
    }

    // ========== MÉTODOS AUXILIARES ==========

    /**
     * Detecta filtros de tipo array (IN, ANY) de manera genérica.
     *
     * @param whereClause Cláusula WHERE
     * @param nombreCampo Nombre del campo SQL a buscar (ej: "id_estado")
     * @param etiqueta Etiqueta legible para UI
     * @param nombreParametro Nombre del parámetro para la query dinámica
     * @return Objeto con resultado de la detección
     */
    private DeteccionFiltro detectarFiltroArray(
            String whereClause,
            String nombreCampo,
            String etiqueta,
            String nombreParametro) {

        // Pattern para IN
        Pattern patternIn = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\." + nombreCampo + ")\\s+(?:NOT\\s+)?IN",
                Pattern.CASE_INSENSITIVE
        );

        // Pattern para valor hardcodeado (= 123)
        Pattern patternEquals = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\." + nombreCampo + ")\\s*=\\s*\\d+",
                Pattern.CASE_INSENSITIVE
        );

        // Pattern para ANY
        Pattern patternAny = Pattern.compile(
                "\\b([a-zA-Z_][a-zA-Z0-9_]*\\." + nombreCampo + ")\\s*=\\s*ANY",
                Pattern.CASE_INSENSITIVE
        );

        String campoSQL = null;
        boolean esHardcodeado = false;

        // Verificar ANY (ya dinámico)
        Matcher matcherAny = patternAny.matcher(whereClause);
        if (matcherAny.find()) {
            campoSQL = matcherAny.group(1);
            log.debug("✓ Filtro {} detectado (ya dinámico con ANY): {}", nombreCampo, campoSQL);
        }
        // Verificar IN
        else {
            Matcher matcherIn = patternIn.matcher(whereClause);
            if (matcherIn.find()) {
                campoSQL = matcherIn.group(1);
                log.debug("✓ Filtro {} detectado (IN): {}", nombreCampo, campoSQL);
            }
            // Verificar hardcodeado
            else {
                Matcher matcherEquals = patternEquals.matcher(whereClause);
                if (matcherEquals.find()) {
                    campoSQL = matcherEquals.group(1);
                    esHardcodeado = true;
                    log.warn("⚠️ Filtro {} detectado HARDCODEADO: {}", nombreCampo, campoSQL);
                }
            }
        }

        if (campoSQL == null) {
            return new DeteccionFiltro(false, null, null, false);
        }

        // Construir metadata
        FiltroMetadata metadata = FiltroMetadata.builder()
                .tipoFiltro("ARRAY_INTEGER")
                .campoSQL(campoSQL)
                .etiqueta(etiqueta)
                .parametros(Arrays.asList(nombreParametro))
                .tipoDato("INTEGER")
                .multiple(true)
                .obligatorio(false)
                .build();

        return new DeteccionFiltro(true, campoSQL, metadata, esHardcodeado);
    }

    /**
     * Genera una etiqueta legible a partir de un nombre de campo SQL.
     *
     * Ejemplos:
     * - "fecha_infraccion" → "Fecha de Infracción"
     * - "id_estado" → "Estado"
     *
     * @param nombreCampo Nombre del campo (snake_case)
     * @return Etiqueta legible (Title Case)
     */
    private String generarEtiquetaLegible(String nombreCampo) {
        // Si el campo contiene "fecha_", generar etiqueta especial
        if (nombreCampo.toLowerCase().contains("fecha_")) {
            String resto = nombreCampo.replaceAll("(?i).*fecha_", "");
            return "Fecha de " + capitalizarPalabras(resto);
        }

        // Remover prefijos comunes
        String limpio = nombreCampo
                .replaceAll("^(id_|cod_)", "")
                .replace("_", " ");

        return capitalizarPalabras(limpio);
    }

    /**
     * Capitaliza cada palabra de un texto.
     */
    private String capitalizarPalabras(String texto) {
        String[] palabras = texto.trim().split("\\s+");
        StringBuilder resultado = new StringBuilder();

        for (int i = 0; i < palabras.length; i++) {
            String palabra = palabras[i];

            if (palabra.length() > 0) {
                resultado.append(Character.toUpperCase(palabra.charAt(0)));

                if (palabra.length() > 1) {
                    resultado.append(palabra.substring(1).toLowerCase());
                }

                if (i < palabras.length - 1) {
                    resultado.append(" ");
                }
            }
        }

        return resultado.toString();
    }

}
