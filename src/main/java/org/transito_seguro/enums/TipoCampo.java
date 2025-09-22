package org.transito_seguro.enums;

public enum TipoCampo {
    UBICACION,          // provincia, municipio, contexto, lugar
    CATEGORIZACION,     // tipo_infraccion, serie_equipo, descripcion
    TIEMPO,            // fecha, fecha_reporte, fecha_emision
    NUMERICO_SUMA,     // total, cantidad, counter, vehiculos
    NUMERICO_COUNT,    // total_sin_email, cantidad_infracciones
    CALCULADO,         // enviado_sacit, hacia_sacit (CASE WHEN)
    IDENTIFICADOR,     // dominio, nro_documento, cuit
    DETALLE           // calle, numero, email, telefono
}
