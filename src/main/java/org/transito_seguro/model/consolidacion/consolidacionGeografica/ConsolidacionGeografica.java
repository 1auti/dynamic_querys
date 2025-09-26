package org.transito_seguro.model.consolidacion.consolidacionGeografica;

import java.util.Optional;

public enum ConsolidacionGeografica {

    PROVINCIA("provincia"),
    CONTEXTO("contexto"),
    MUNICIPIO("municipio");

    private final String valor;

    ConsolidacionGeografica(String valor) {
        this.valor = valor;
    }

    public String getValor() {
        return valor;
    }

    public static Optional<ConsolidacionGeografica> fromString(String texto) {
        for (ConsolidacionGeografica n : values()) {
            if (n.valor.equalsIgnoreCase(texto)) {
                return Optional.of(n);
            }
        }
        return Optional.empty();
    }

}
