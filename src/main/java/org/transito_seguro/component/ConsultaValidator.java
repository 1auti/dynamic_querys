package org.transito_seguro.component;


import org.springframework.stereotype.Component;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import javax.xml.bind.ValidationException;
import java.util.*;

@Component
public class ConsultaValidator {

    private final Set<String> baseDatosSoportadas = new HashSet<>(Arrays.asList(
            "pba", "mda", "santa_rosa", "chaco", "entre_rios", "formosa"
    ));

    public void validarConsulta(ConsultaQueryDTO consulta) throws ValidationException {
        List<String> errores = new ArrayList<>();

        validarFechas(consulta.getParametrosFiltros(), errores);
        validarProvincias(consulta.getParametrosFiltros(), errores);
        validarFormato(consulta.getFormato(), errores);

        if (!errores.isEmpty()) {
            throw new ValidationException("Errores de validación: " + String.join(", ", errores));
        }
    }

    private void validarFechas(ParametrosFiltrosDTO filtros, List<String> errores) {
        boolean tieneRango = filtros.getFechaInicio() != null || filtros.getFechaFin() != null;
        boolean tieneEspecifica = filtros.getFechaEspecifica() != null;

        if (tieneRango && tieneEspecifica) {
            errores.add("No se puede usar fecha específica y rango al mismo tiempo");
        }

        if (filtros.getFechaInicio() != null && filtros.getFechaFin() != null) {
            if (filtros.getFechaInicio().after(filtros.getFechaFin())) {
                errores.add("La fecha de inicio debe ser anterior a la fecha fin");
            }
        }
    }

    private void validarProvincias(ParametrosFiltrosDTO filtros, List<String> errores) {
        if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {
            for (String provincia : filtros.getBaseDatos()) {
                if (!baseDatosSoportadas.contains(provincia)) {
                    errores.add("Provincia no soportada: " + provincia);
                }
            }
        }
    }

    private void validarFormato(String formato, List<String> errores) {
        if (formato != null && !Arrays.asList("json", "csv", "excel").contains(formato.toLowerCase())) {
            errores.add("Formato no soportado: " + formato);
        }
    }
}
