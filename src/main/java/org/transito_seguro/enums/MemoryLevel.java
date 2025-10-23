package org.transito_seguro.enums;

import lombok.Getter;

public enum MemoryLevel {
    LOW("🟢 BAJO", "Memoria en nivel óptimo"),
    NORMAL("🟡 NORMAL", "Memoria en nivel normal"),
    HIGH("🟠 ALTO", "Memoria alta - Se recomienda pausa"),
    CRITICAL("🔴 CRÍTICO", "Memoria crítica - Acción inmediata requerida");

    @Getter
    private final String emoji;

    @Getter
    private final String descripcion;

    MemoryLevel(String emoji, String descripcion) {
        this.emoji = emoji;
        this.descripcion = descripcion;
    }

    @Override
    public String toString() {
        return emoji + " " + name();
    }
}