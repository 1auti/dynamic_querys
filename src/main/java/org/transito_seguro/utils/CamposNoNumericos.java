package org.transito_seguro.utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public final class CamposNoNumericos {

    public static final Set<String> CAMPOS_NO_NUMERICOS;

    static {
        Set<String> campos = new HashSet<>();
        campos.add("provincia");
        campos.add("provincia_origen");
        campos.add("contexto");
        campos.add("municipio");
        campos.add("descripcion");
        campos.add("dominio");
        campos.add("tipo_documento");
        campos.add("nro_documento");
        campos.add("propietario_apellido");
        campos.add("propietario_nombre");
        campos.add("fecha");
        campos.add("fecha_titularidad");
        campos.add("calle");
        campos.add("numero");
        campos.add("piso");
        campos.add("departamento");
        campos.add("cp");
        campos.add("localidad");
        campos.add("telefono");
        campos.add("celular");
        campos.add("email");
        campos.add("marca");
        campos.add("modelo");
        campos.add("vehiculo");
        campos.add("partido");
        campos.add("fecha_alta");
        campos.add("fecha_reporte");
        campos.add("tipo_infraccion");
        campos.add("estado");
        campos.add("serie_equipo");
        campos.add("lugar");
        campos.add("enviado_sacit");
        campos.add("fecha_emision");
        campos.add("hacia_sacit");
        campos.add("ultima_modificacion");
        campos.add("fecha_constatacion");
        campos.add("mes_exportacion");
        campos.add("packedfile");
        campos.add("fuente_consolidacion");
        campos.add("error_consolidacion");
        campos.add("mensaje_error");
        campos.add("timestamp_error");

        // Normalizamos todo a minúsculas e inmutable
        CAMPOS_NO_NUMERICOS = Collections.unmodifiableSet(
                campos.stream()
                        .map(c -> c.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet())
        );
    }

    public static boolean esNoNumerico(String campo) {
        return campo != null && CAMPOS_NO_NUMERICOS.contains(campo.toLowerCase());
    }
}
