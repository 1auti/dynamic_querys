package org.transito_seguro.enums;

public enum EstrategiaPaginacion {

    KEYSET_CON_ID, // Keyset usando i.id + 3 campos adicionales
    KEY_COMPUESTO, // Keyset compuesto
    KEYSET_CONSOLIDADO,
    OFFSET, // Paginacion tradicional
    SIN_PAGINACION, // No aplicar paginacion ( querys consolidacion )
    FALLBACK_LIMIT_ONLY // Solo limit sin condiciones keyset
}
