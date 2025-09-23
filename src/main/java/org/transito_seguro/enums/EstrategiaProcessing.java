package org.transito_seguro.enums;

public enum EstrategiaProcessing {
    PARALELO, // Pequeño - Mediano  < 50k x provincia
    HIBRIDO, // Intermedio 50K - 200K  x provincia
    SECUENCIAL // Masivo > 200k x provincia
}
