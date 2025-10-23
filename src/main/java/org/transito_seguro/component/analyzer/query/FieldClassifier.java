package org.transito_seguro.component.analyzer.query;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.model.CampoAnalizado;

import java.util.*;
import java.util.regex.Matcher;

import static org.transito_seguro.enums.TipoCampo.determinarTipoCampo;
import static org.transito_seguro.utils.RegexUtils.*;

/**
 * Componente especializado en la clasificación de campos SQL.
 *
 * Responsabilidades:
 * - Analizar expresiones SQL (funciones, alias, cálculos)
 * - Determinar tipo de campo (UBICACION, TIEMPO, NUMERICO, etc.)
 * - Inferir tipo por nombre cuando no hay expresión clara
 * - Gestionar cardinalidades conocidas
 */
@Slf4j
@Component
public class FieldClassifier {


    // ========== CARDINALIDADES CONOCIDAS ==========
    private static final Map<String, Integer> CARDINALIDADES_CONOCIDAS;

    static {
        Map<String, Integer> map = new HashMap<>();

        // UBICACIÓN
        map.put("provincia", 24);
        map.put("municipio", 500);
        map.put("localidad", 2_000);
        map.put("lugar", 5_000);
        map.put("ciudad", 1_000);
        map.put("departamento", 300);
        map.put("region", 10);

        // TIEMPO
        map.put("anio", 5);
        map.put("mes", 12);
        map.put("dia", 31);
        map.put("trimestre", 4);
        map.put("semestre", 2);
        map.put("dia_semana", 7);
        map.put("fecha", 365);
        map.put("hora", 24);
        map.put("mes_anio", 60);
        map.put("fecha_constatacion", 365);

        // CATEGORIZACIÓN
        map.put("tipo_infraccion", 50);
        map.put("tipo", 20);
        map.put("estado", 5);
        map.put("estado_infraccion", 10);
        map.put("enviado_a", 3);
        map.put("categoria", 10);
        map.put("gravedad", 3);

        // EQUIPOS
        map.put("serie_equipo", 100);

        // IDENTIFICADORES
        map.put("id", 10_000);
        map.put("codigo", 1_000);

        // COMBINACIONES
        map.put("provincia_mes", 288);
        map.put("provincia_anio", 120);
        map.put("provincia_tipo", 1_200);
        map.put("mes_tipo", 600);
        map.put("municipio_localidad", 10_000);
        map.put("serie_equipo_tipo", 5_000);
        map.put("localidad_tipo_infraccion", 100_000);

        CARDINALIDADES_CONOCIDAS = Collections.unmodifiableMap(map);
    }

    /**
     * Analiza un campo individual del SELECT.
     *
     * @param expresion Expresión del campo (ej: "SUM(monto) AS total")
     * @return Campo analizado con metadatos
     */
    public CampoAnalizado analizarCampoIndividual(String expresion) {
        // Detectar alias con AS
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        String expresionLimpia;
        String nombreFinal;

        if (aliasMatcher.matches()) {
            expresionLimpia = aliasMatcher.group(1).trim();
            nombreFinal = aliasMatcher.group(2).trim().toLowerCase();
            nombreFinal = nombreFinal.replaceAll("[\"'`]", "");
        } else {
            expresionLimpia = expresion.trim();
            nombreFinal = expresionLimpia
                    .replaceAll("^[a-zA-Z_]+\\.", "")
                    .toLowerCase();
        }

        // Determinar tipo
        TipoCampo tipo = determinarTipoCampo(expresionLimpia, nombreFinal);
        if (tipo == null) {
            tipo = inferirTipoPorNombre(nombreFinal);
        }

        boolean esAgregacion = esExpresionAgregacion(expresionLimpia);
        boolean esCalculado = esExpresionCalculada(expresionLimpia);

        return new CampoAnalizado(
                expresion,
                expresionLimpia,
                nombreFinal,
                tipo,
                esAgregacion,
                esCalculado
        );
    }

    /**
     * Infiere el tipo de un campo basándose en su nombre.
     *
     * @param nombre Nombre del campo (normalizado)
     * @return Tipo de campo inferido
     */
    public TipoCampo inferirTipoPorNombre(String nombre) {
        String nombreLower = nombre.toLowerCase();

        // IDENTIFICADORES
        if (nombreLower.matches(".*_(id|codigo|serie|clave|key)$") ||
                nombreLower.startsWith("id_") ||
                nombreLower.equals("id")) {
            return TipoCampo.IDENTIFICADOR;
        }

        // UBICACIÓN
        if (nombreLower.contains("provincia") ||
                nombreLower.contains("municipio") ||
                nombreLower.contains("localidad") ||
                nombreLower.contains("ciudad") ||
                nombreLower.contains("departamento") ||
                nombreLower.contains("region") ||
                nombreLower.contains("lugar") ||
                nombreLower.contains("ubicacion")) {
            return TipoCampo.UBICACION;
        }

        // TIEMPO
        if (nombreLower.contains("fecha") ||
                nombreLower.contains("anio") ||
                nombreLower.contains("mes") ||
                nombreLower.contains("dia") ||
                nombreLower.contains("hora") ||
                nombreLower.contains("trimestre") ||
                nombreLower.contains("periodo")) {
            return TipoCampo.TIEMPO;
        }

        // CATEGORIZACIÓN
        if (nombreLower.contains("tipo") ||
                nombreLower.contains("estado") ||
                nombreLower.contains("categoria") ||
                nombreLower.contains("gravedad") ||
                nombreLower.contains("nivel")) {
            return TipoCampo.CATEGORIZACION;
        }

        // NUMÉRICOS
        if (nombreLower.matches("(total|suma|count|cantidad|monto|valor).*") ||
                nombreLower.startsWith("num_") ||
                nombreLower.startsWith("cant_")) {
            return TipoCampo.NUMERICO_SUMA;
        }

        // DESCRIPCIONES
        if (nombreLower.contains("descripcion") ||
                nombreLower.contains("detalle") ||
                nombreLower.contains("observacion")) {
            return TipoCampo.DETALLE;
        }

        log.debug("Campo '{}' no coincide con patrones - Tipo DETALLE por defecto", nombre);
        return TipoCampo.DETALLE;
    }

    /**
     * Obtiene la cardinalidad conocida de un campo.
     *
     * @param nombreCampo Nombre del campo
     * @return Cardinalidad conocida o null si no existe
     */
    public Integer obtenerCardinalidadConocida(String nombreCampo) {
        return CARDINALIDADES_CONOCIDAS.get(nombreCampo);
    }

    /**
     * Estima la cardinalidad de un campo desconocido basándose en su tipo.
     *
     * @param campo Nombre del campo
     * @param tipo Tipo del campo
     * @return Cardinalidad estimada
     */
    public int estimarCardinalidadPorTipo(String campo, TipoCampo tipo) {
        if (tipo == null) {
            log.warn("Campo sin tipo: {} - Usando 100", campo);
            return 100;
        }

        switch (tipo) {
            case UBICACION:
                return 500;
            case TIEMPO:
                return 365;
            case CATEGORIZACION:
                return 20;
            case IDENTIFICADOR:
                return 1_000;
            case NUMERICO_SUMA:
            case NUMERICO_COUNT:
                return 100;
            default:
                return 100;
        }
    }

    // Métodos auxiliares
    private boolean esExpresionAgregacion(String expresion) {
        return expresion.toUpperCase().matches(".*(SUM|COUNT|AVG|MAX|MIN|STRING_AGG)\\s*\\(.*");
    }

    private boolean esExpresionCalculada(String expresion) {
        return expresion.contains("CASE") ||
                expresion.contains("CONCAT") ||
                expresion.contains("COALESCE") ||
                expresion.matches(".*[+\\-*/].*");
    }

    public static Map<String, Integer> getCardinalidadesConocidas() {
        return CARDINALIDADES_CONOCIDAS;
    }
}
