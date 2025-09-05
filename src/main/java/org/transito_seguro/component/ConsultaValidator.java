package org.transito_seguro.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.transito_seguro.config.ProvinciaMapping;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import javax.xml.bind.ValidationException;
import java.util.*;

@Component
public class ConsultaValidator {

    @Autowired
    private ProvinciaMapping provinciaMapping;

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

    /**
     * Validación mejorada que acepta tanto nombres de provincias como códigos de datasource
     */
    private void validarProvincias(ParametrosFiltrosDTO filtros, List<String> errores) {
        if (filtros.getBaseDatos() != null && !filtros.getBaseDatos().isEmpty()) {

            // Obtener todas las provincias y códigos válidos desde el mapping
            Set<String> provinciasValidas = new HashSet<>();
            Set<String> codigosValidos = new HashSet<>();

            if (provinciaMapping.getMapping() != null) {
                provinciasValidas.addAll(provinciaMapping.getMapping().keySet()); // "Buenos Aires", "Avellaneda", etc.
                codigosValidos.addAll(provinciaMapping.getMapping().values());   // "pba", "mda", etc.
            }

            for (String provincia : filtros.getBaseDatos()) {
                boolean esProvinciaValida = provinciasValidas.contains(provincia);
                boolean esCodigoValido = codigosValidos.contains(provincia);

                if (!esProvinciaValida && !esCodigoValido) {
                    errores.add("Provincia/código no soportado: " + provincia +
                            ". Válidos: " + String.join(", ", provinciasValidas) +
                            " o códigos: " + String.join(", ", codigosValidos));
                }
            }
        }
    }

    private void validarFormato(String formato, List<String> errores) {
        if (formato != null && !Arrays.asList("json", "csv", "excel").contains(formato.toLowerCase())) {
            errores.add("Formato no soportado: " + formato);
        }
    }

    /**
     * Método utilitario para normalizar nombres de provincias/códigos
     * Convierte nombres de provincias a códigos de datasource
     */
    public List<String> normalizarProvincias(List<String> provinciasOriginales) {
        if (provinciasOriginales == null || provinciasOriginales.isEmpty()) {
            return provinciasOriginales;
        }

        List<String> provinciasNormalizadas = new ArrayList<>();
        Map<String, String> mapping = provinciaMapping.getMapping();

        if (mapping == null) {
            return provinciasOriginales; // Fallback
        }

        // Crear mapeo inverso (código -> nombre) para búsqueda bidireccional
        Map<String, String> mappingInverso = new HashMap<>();
        mapping.forEach((nombre, codigo) -> mappingInverso.put(codigo, nombre));

        for (String provincia : provinciasOriginales) {
            if (mapping.containsKey(provincia)) {
                // Es un nombre de provincia, convertir a código
                provinciasNormalizadas.add(mapping.get(provincia));
            } else if (mappingInverso.containsKey(provincia)) {
                // Ya es un código, mantenerlo
                provinciasNormalizadas.add(provincia);
            } else {
                // No válido, mantenerlo para que la validación lo capture
                provinciasNormalizadas.add(provincia);
            }
        }

        return provinciasNormalizadas;
    }

    /**
     * Obtiene todas las provincias soportadas (nombres completos)
     */
    public Set<String> getProvinciasPermitidas() {
        return provinciaMapping.getMapping() != null ?
                provinciaMapping.getMapping().keySet() :
                Collections.emptySet();
    }

    /**
     * Obtiene todos los códigos de datasource soportados
     */
    public Set<String> getCodigosPermitidos() {
        return provinciaMapping.getMapping() != null ?
                new HashSet<>(provinciaMapping.getMapping().values()) :
                Collections.emptySet();
    }
}