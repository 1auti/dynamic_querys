package org.transito_seguro.enums;

import org.transito_seguro.domain.CamposPorCategoria;
import org.transito_seguro.utils.RegexUtils;

/**
 * Enum mejorado para clasificación automática de campos en SQL
 * Usado por DynamicBuilderQuery para determinar filtros dinámicos
 */
public enum TipoCampo {

    // =============== TIPOS GEOGRÁFICOS ===============
    UBICACION("Campos geográficos como provincia, municipio, contexto"),

    // =============== TIPOS DE CATEGORIZACIÓN ===============
    CATEGORIZACION("Campos de clasificación como tipo_infraccion, serie_equipo"),

    // =============== TIPOS TEMPORALES ===============
    TIEMPO("Campos de fecha y tiempo como fecha_alta, timestamp"),

    // =============== TIPOS NUMÉRICOS ===============
    NUMERICO_SUMA("Campos numéricos agregables con SUM como total, cantidad"),
    NUMERICO_COUNT("Campos de conteo como total_sin_email, cantidad_infracciones"),

    // =============== TIPOS DE LÓGICA ===============
    CALCULADO("Campos con lógica CASE WHEN como enviado_sacit, hacia_sacit"),

    // =============== TIPOS DE IDENTIFICACIÓN ===============
    IDENTIFICADOR("Campos únicos como id, codigo, serial, dominio, nro_documento"),

    // =============== TIPOS DE DETALLE ===============
    DETALLE("Campos de información detallada como email, telefono, direccion"),

    // =============== NUEVOS TIPOS PARA FILTRADO DINÁMICO ===============
    BOOLEANO("Campos boolean como activo, eliminado, habilitado"),
    TEXTO("Campos de texto libre como descripcion, observaciones"),
    ESTADO("Campos de estado/enum como estado_infraccion, status"),
    EMAIL("Campos de email específicamente"),
    TELEFONO("Campos de teléfono/celular");

    private final String descripcion;

    TipoCampo(String descripcion) {
        this.descripcion = descripcion;
    }

    public String getDescripcion() {
        return descripcion;
    }


    public static TipoCampo determinarTipoCampo(String expresion, String nombreFinal) {
        if (CamposPorCategoria.CAMPOS_UBICACION.containsKey(nombreFinal)) return TipoCampo.UBICACION;
        if (CamposPorCategoria.CAMPOS_CATEGORIZACION.containsKey(nombreFinal)) return TipoCampo.CATEGORIZACION;
        if (CamposPorCategoria.CAMPOS_TIEMPO.containsKey(nombreFinal) || nombreFinal.contains("fecha")) return TipoCampo.TIEMPO;
        if (CamposPorCategoria.CAMPOS_NUMERICOS_CONOCIDOS.containsKey(nombreFinal)) return TipoCampo.NUMERICO_SUMA;

        if (RegexUtils.SUM_PATTERN.matcher(expresion).find()) return TipoCampo.NUMERICO_SUMA;
        if (RegexUtils.COUNT_PATTERN.matcher(expresion).find()) return TipoCampo.NUMERICO_COUNT;
        if (RegexUtils.CASE_WHEN_PATTERN.matcher(expresion).find()) return TipoCampo.CALCULADO;

        if (nombreFinal.contains("total") || nombreFinal.contains("cantidad") ||
                nombreFinal.contains("counter") || nombreFinal.endsWith("_count")) return TipoCampo.NUMERICO_SUMA;

        if (nombreFinal.contains("dominio") || nombreFinal.contains("documento") ||
                nombreFinal.contains("cuit") || nombreFinal.contains("nro_")) return TipoCampo.IDENTIFICADOR;

        return TipoCampo.DETALLE;
    }

    public static String determinarOperadorDefault(TipoCampo tipo) {
        switch (tipo) {
            case IDENTIFICADOR:
            case NUMERICO_SUMA:
            case NUMERICO_COUNT:
            case CALCULADO:
                return "=";
            case TIEMPO:
                return ">=";
            case UBICACION:
            case CATEGORIZACION:
            case DETALLE:
            default:
                return "ILIKE";
        }
    }
}