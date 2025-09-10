package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConsolidacionService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    /**
     * Consolida datos de múltiples provincias en un solo resultado
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación de datos para {} provincias", repositories.size());

        List<Map<String, Object>> datosConsolidados = new ArrayList<>();
        int totalProcesados = 0;

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                log.debug("Obteniendo datos de provincia: {}", provincia);

                List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar información de provincia origen a cada registro
                    datosProvider.forEach(registro -> {
                        registro.put("provincia_origen", provincia);
                        registro.put("fuente_consolidacion", true);
                    });

                    datosConsolidados.addAll(datosProvider);
                    totalProcesados += datosProvider.size();

                    log.debug("Provincia {}: {} registros obtenidos", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error obteniendo datos de provincia {}: {}", provincia, e.getMessage(), e);

                // Agregar registro de error para tracking
                Map<String, Object> registroError = new HashMap<>();
                registroError.put("provincia_origen", provincia);
                registroError.put("error_consolidacion", true);
                registroError.put("mensaje_error", e.getMessage());
                registroError.put("timestamp_error", new Date());

                datosConsolidados.add(registroError);
            }
        }

        log.info("Consolidación completada: {} registros de {} provincias",
                totalProcesados, repositories.size());

        return procesarConsolidacion(datosConsolidados, filtros);
    }

    /**
     * Procesa y organiza los datos consolidados
     */
    private List<Map<String, Object>> procesarConsolidacion(
            List<Map<String, Object>> datosRaw,
            ParametrosFiltrosDTO filtros) {

        if (datosRaw == null || datosRaw.isEmpty()) {
            log.info("No hay datos para procesar en consolidación");
            return Collections.emptyList();
        }

        log.debug("Procesando consolidación de {} registros", datosRaw.size());

        List<Map<String, Object>> datosConsolidados = new ArrayList<>(datosRaw);

        // Aplicar ordenamiento por provincia
        datosConsolidados.sort((a, b) -> {
            String provinciaA = (String) a.getOrDefault("provincia_origen", "");
            String provinciaB = (String) b.getOrDefault("provincia_origen", "");
            return provinciaA.compareTo(provinciaB);
        });

        // Agregar metadata de consolidación
        agregarMetadataConsolidacion(datosConsolidados);

        // Aplicar límites si es necesario
        return aplicarLimitesConsolidacion(datosConsolidados, filtros);
    }

    /**
     * Agrega metadata de consolidación a los registros
     */
    private void agregarMetadataConsolidacion(List<Map<String, Object>> datos) {
        // Contar registros por provincia
        Map<String, Long> conteosPorProvincia = datos.stream()
                .filter(registro -> !Boolean.TRUE.equals(registro.get("error_consolidacion")))
                .collect(Collectors.groupingBy(
                        registro -> (String) registro.getOrDefault("provincia_origen", "Desconocida"),
                        Collectors.counting()
                ));

        // Agregar información de consolidación al primer registro (como header)
        if (!datos.isEmpty()) {
            Map<String, Object> primerRegistro = datos.get(0);
            primerRegistro.put("consolidacion_metadata", conteosPorProvincia);
            primerRegistro.put("total_provincias", conteosPorProvincia.size());
            primerRegistro.put("total_registros_consolidados", datos.size());
            primerRegistro.put("fecha_consolidacion", new Date());
        }

        log.info("Metadata de consolidación agregada: {} provincias, conteos: {}",
                conteosPorProvincia.size(), conteosPorProvincia);
    }

    /**
     * Aplica límites específicos para datos consolidados
     */
    private List<Map<String, Object>> aplicarLimitesConsolidacion(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        log.debug("Aplicando límites consolidación: limite={}, offset={}, total={}",
                limite, offset, datos.size());

        // Aplicar offset
        if (offset > 0 && offset < datos.size()) {
            datos = datos.subList(offset, datos.size());
        } else if (offset >= datos.size()) {
            log.warn("Offset {} excede tamaño de datos {}", offset, datos.size());
            return Collections.emptyList();
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        log.debug("Límites aplicados: {} registros finales", datos.size());
        return datos;
    }

    /**
     * Consolida datos con agrupación por criterios específicos
     */
    public List<Map<String, Object>> consolidarConAgrupacion(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros,
            String campoAgrupacion) {

        log.info("Iniciando consolidación con agrupación por campo: {}", campoAgrupacion);

        List<Map<String, Object>> datosConsolidados = consolidarDatos(repositories, nombreQuery, filtros);

        if (campoAgrupacion == null || campoAgrupacion.trim().isEmpty()) {
            return datosConsolidados;
        }

        // Agrupar por el campo especificado
        Map<Object, List<Map<String, Object>>> datosAgrupados = datosConsolidados.stream()
                .filter(registro -> registro.containsKey(campoAgrupacion))
                .collect(Collectors.groupingBy(registro -> registro.get(campoAgrupacion)));

        // Convertir grupos en registros consolidados
        List<Map<String, Object>> resultadoAgrupado = new ArrayList<>();

        for (Map.Entry<Object, List<Map<String, Object>>> grupo : datosAgrupados.entrySet()) {
            Map<String, Object> registroAgrupado = new HashMap<>();

            registroAgrupado.put(campoAgrupacion, grupo.getKey());
            registroAgrupado.put("total_registros_grupo", grupo.getValue().size());
            registroAgrupado.put("provincias_en_grupo", grupo.getValue().stream()
                    .map(r -> r.get("provincia_origen"))
                    .distinct()
                    .collect(Collectors.toList()));
            registroAgrupado.put("datos_detalle", grupo.getValue());
            registroAgrupado.put("agrupado_por", campoAgrupacion);

            resultadoAgrupado.add(registroAgrupado);
        }

        log.info("Consolidación con agrupación completada: {} grupos creados", resultadoAgrupado.size());
        return resultadoAgrupado;
    }

    /**
     * Genera un resumen estadístico de la consolidación
     */
    public Map<String, Object> generarResumenConsolidacion(
            List<Map<String, Object>> datosConsolidados) {

        Map<String, Object> resumen = new HashMap<>();

        if (datosConsolidados == null || datosConsolidados.isEmpty()) {
            resumen.put("total_registros", 0);
            resumen.put("provincias", Collections.emptyList());
            return resumen;
        }

        // Contar registros totales (excluyendo errores)
        long totalRegistros = datosConsolidados.stream()
                .filter(registro -> !Boolean.TRUE.equals(registro.get("error_consolidacion")))
                .count();

        // Obtener provincias únicas
        Set<String> provincias = datosConsolidados.stream()
                .map(registro -> (String) registro.get("provincia_origen"))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        // Contar errores
        long totalErrores = datosConsolidados.stream()
                .filter(registro -> Boolean.TRUE.equals(registro.get("error_consolidacion")))
                .count();

        resumen.put("total_registros", totalRegistros);
        resumen.put("total_provincias", provincias.size());
        resumen.put("provincias", new ArrayList<>(provincias));
        resumen.put("total_errores", totalErrores);
        resumen.put("fecha_resumen", new Date());

        // Distribución por provincia
        Map<String, Long> distribucion = datosConsolidados.stream()
                .filter(registro -> !Boolean.TRUE.equals(registro.get("error_consolidacion")))
                .collect(Collectors.groupingBy(
                        registro -> (String) registro.getOrDefault("provincia_origen", "Desconocida"),
                        Collectors.counting()
                ));

        resumen.put("distribucion_por_provincia", distribucion);

        log.debug("Resumen de consolidación generado: {} registros, {} provincias",
                totalRegistros, provincias.size());

        return resumen;
    }

    /**
     * Valida si la consolidación es posible con los filtros dados
     */
    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        if (!filtros.esConsolidado()) {
            return false;
        }

        // Verificar que hay repositorios disponibles
        Map<String, ?> repositories = repositoryFactory.getAllRepositories();
        if (repositories.isEmpty()) {
            log.warn("No hay repositorios disponibles para consolidación");
            return false;
        }

        log.debug("Consolidación validada: {} repositorios disponibles", repositories.size());
        return true;
    }

    /**
     * Obtiene información sobre las provincias disponibles para consolidación
     */
    public Map<String, Object> obtenerInfoConsolidacion() {
        Map<String, Object> info = new HashMap<>();

        Set<String> provinciasDisponibles = repositoryFactory.getProvinciasSoportadas();
        Map<String, ?> repositories = repositoryFactory.getAllRepositories();

        info.put("provincias_disponibles", new ArrayList<>(provinciasDisponibles));
        info.put("total_provincias", provinciasDisponibles.size());
        info.put("repositories_activos", repositories.size());
        info.put("consolidacion_habilitada", !repositories.isEmpty());

        return info;
    }
}