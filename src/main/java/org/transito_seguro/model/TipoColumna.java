package org.transito_seguro.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Información sobre el tipo y características de una columna
 */
@Getter
@Setter
public class TipoColumna {
    private boolean esNumerico = false;
    private boolean esTexto = false;
    private boolean esFecha = false;
    private boolean esUnico = false; // Valores únicos en > 80% de registros
    private Set<Object> valoresUnicos = new HashSet<>();
    private int totalRegistros = 0;
    private Pattern patron = null; // Para detectar patrones como dominios, fechas, etc.


}
