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
     * Consolida datos AGREGANDO/SUMANDO valores de múltiples provincias
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación AGREGADA de datos para {} provincias", repositories.size());

        // 1. Recopilar todos los datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatosDeTodosLosRepositories(repositories, nombreQuery, filtros);

        if (todosLosDatos.isEmpty()) {
            log.info("No hay datos para consolidar");
            return Collections.emptyList();
        }

        // 2. Determinar el tipo de consolidación según la query
        String tipoConsolidacion = determinarTipoConsolidacion(nombreQuery, todosLosDatos);

        // 3. Aplicar la consolidación específica
        List<Map<String, Object>> datosConsolidados = aplicarConsolidacion(todosLosDatos, tipoConsolidacion);

        // 4. Agregar metadata de consolidación
        //agregarMetadataConsolidacion(datosConsolidados, repositories);

        // 5. Aplicar límites si es necesario
        return aplicarLimitesConsolidacion(datosConsolidados, filtros);
    }

    /**
     * Recopila datos de todos los repositorios
     */
    private List<Map<String, Object>> recopilarDatosDeTodosLosRepositories(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();
        Set<String> provinciasConErrores = new HashSet<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                log.debug("Obteniendo datos de provincia: {}", provincia);

                List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar información de provincia origen a cada registro
                    datosProvider.forEach(registro -> {
                        registro.put("provincia_origen", provincia);
                    });

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Provincia {}: {} registros obtenidos", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error obteniendo datos de provincia {}: {}", provincia, e.getMessage(), e);
                provinciasConErrores.add(provincia);
            }
        }

        if (!provinciasConErrores.isEmpty()) {
            log.warn("Provincias con errores en consolidación: {}", provinciasConErrores);
        }

        log.info("Recopilación completada: {} registros totales de {} provincias",
                todosLosDatos.size(), repositories.size() - provinciasConErrores.size());

        return todosLosDatos;
    }

    /**
     * Determina el tipo de consolidación basado en la query y estructura de datos
     */
    private String determinarTipoConsolidacion(String nombreQuery, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return "default";
        }

        // Analizar la primera fila para determinar estructura
        Map<String, Object> primeraFila = datos.get(0);
        Set<String> columnas = primeraFila.keySet();

        // Determinar tipo según query y columnas disponibles
        if (nombreQuery.contains("personas_juridicas")) {
            return "personas_juridicas"; // No se suma, se mantiene por dominio único
        } else if (nombreQuery.contains("reporte_general") || nombreQuery.contains("detallado")) {
            return "reporte_infracciones"; // Suma por fecha + municipio + tipo
        } else if (nombreQuery.contains("radar_fijo") || nombreQuery.contains("semaforo")) {
            return "reporte_equipos"; // Suma por fecha + municipio + equipo
        } else if (nombreQuery.contains("vehiculos_municipio")) {
            return "vehiculos_municipio"; // Suma totales por municipio
        } else if (nombreQuery.contains("sin_email")) {
            return "sin_email"; // Suma conteos por municipio
        } else if (nombreQuery.contains("estado")) {
            return "infracciones_estado"; // Suma por estado
        } else {
            // Consolidación genérica basada en columnas numéricas
            return "consolidacion_generica";
        }
    }

    /**
     * Aplica la consolidación específica según el tipo
     */
    private List<Map<String, Object>> aplicarConsolidacion(List<Map<String, Object>> datos, String tipoConsolidacion) {

        log.info("Aplicando consolidación tipo: {} a {} registros", tipoConsolidacion, datos.size());

        switch (tipoConsolidacion) {
            case "personas_juridicas":
                return consolidarPersonasJuridicas(datos);
            case "reporte_infracciones":
                return consolidarReporteInfracciones(datos);
            case "reporte_equipos":
                return consolidarReporteEquipos(datos);
            case "vehiculos_municipio":
                return consolidarVehiculosMunicipio(datos);
            case "sin_email":
                return consolidarSinEmail(datos);
            case "infracciones_estado":
                return consolidarInfraccionesEstado(datos);
            case "consolidacion_generica":
            default:
                return consolidarGenerico(datos);
        }
    }

    /**
     * Consolidación para personas jurídicas (mantiene registros únicos por dominio)
     */
    private List<Map<String, Object>> consolidarPersonasJuridicas(List<Map<String, Object>> datos) {
        // Para personas jurídicas, no sumamos - eliminamos duplicados por dominio
        Map<String, Map<String, Object>> registrosUnicos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String dominio = (String) registro.get("DOMINIO");
            if (dominio != null && !registrosUnicos.containsKey(dominio)) {
                // Agregar lista de provincias donde se encontró
                Set<String> provincias = datos.stream()
                        .filter(r -> dominio.equals(r.get("DOMINIO")))
                        .map(r -> (String) r.get("provincia_origen"))
                        .collect(Collectors.toSet());

                registro.put("provincias_encontrado", new ArrayList<>(provincias));
                registro.put("consolidado", true);
                registrosUnicos.put(dominio, registro);
            }
        }

        log.info("Personas jurídicas consolidadas: {} registros únicos de {} originales",
                registrosUnicos.size(), datos.size());

        return new ArrayList<>(registrosUnicos.values());
    }

    /**
     * Consolidación para reportes de infracciones (suma por fecha + municipio + tipo)
     */
    private List<Map<String, Object>> consolidarReporteInfracciones(List<Map<String, Object>> datos) {
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // Crear clave compuesta para agrupación
            String clave = crearClaveConsolidacion(registro, Arrays.asList("fecha", "municipio", "tipo_infraccion", "enviado_sacit"));

            if (registrosConsolidados.containsKey(clave)) {
                // Sumar valores numéricos existentes
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);
                sumarValoresNumericos(registroExistente, registro);
                agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
            } else {
                // Nuevo registro consolidado
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                Set<String> provincias = new HashSet<>();
                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
                nuevoRegistro.put("consolidado", true);
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        log.info("Infracciones consolidadas: {} registros de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación para reportes de equipos (suma por fecha + municipio + equipo)
     */
    private List<Map<String, Object>> consolidarReporteEquipos(List<Map<String, Object>> datos) {
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // Crear clave para equipos
            String clave = crearClaveConsolidacion(registro, Arrays.asList("fecha", "municipio", "serie_equipo", "lugar"));

            if (registrosConsolidados.containsKey(clave)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);
                sumarValoresNumericos(registroExistente, registro);
                agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                Set<String> provincias = new HashSet<>();
                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
                nuevoRegistro.put("consolidado", true);
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        log.info("Equipos consolidados: {} registros de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación para vehículos por municipio (suma totales por municipio)
     */
    private List<Map<String, Object>> consolidarVehiculosMunicipio(List<Map<String, Object>> datos) {
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String clave = crearClaveConsolidacion(registro, Arrays.asList("municipio", "fecha_reporte"));

            if (registrosConsolidados.containsKey(clave)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);

                // Sumar específicamente vehiculos, motos, formato_no_valido, total
                sumarCampoNumerico(registroExistente, registro, "vehiculos");
                sumarCampoNumerico(registroExistente, registro, "motos");
                sumarCampoNumerico(registroExistente, registro, "formato_no_valido");
                sumarCampoNumerico(registroExistente, registro, "total");

                agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                Set<String> provincias = new HashSet<>();
                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
                nuevoRegistro.put("consolidado", true);
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        log.info("Vehículos por municipio consolidados: {} registros de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación para reportes sin email (suma conteos por municipio)
     */
    private List<Map<String, Object>> consolidarSinEmail(List<Map<String, Object>> datos) {
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String clave = crearClaveConsolidacion(registro, Arrays.asList("municipio", "fecha_reporte"));

            if (registrosConsolidados.containsKey(clave)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);
                sumarCampoNumerico(registroExistente, registro, "total_sin_email");
                agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                Set<String> provincias = new HashSet<>();
                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
                nuevoRegistro.put("consolidado", true);
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        log.info("Sin email consolidados: {} registros de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación para infracciones por estado (suma por estado + municipio)
     */
    private List<Map<String, Object>> consolidarInfraccionesEstado(List<Map<String, Object>> datos) {
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // SOLO POR ESTADO (sin municipio)
            String estado = String.valueOf(registro.getOrDefault("estado", "")).trim();

            if (estado.isEmpty()) continue;

            if (registrosConsolidados.containsKey(estado)) {
                // SUMAR al estado existente
                Map<String, Object> existente = registrosConsolidados.get(estado);
                sumarCampoNumerico(existente, registro, "cantidad_infracciones");
//                agregarAConjunto(existente, "provincias_involucradas",
//                        (String) registro.get("provincia_origen"));
//                agregarAConjunto(existente, "municipios_involucrados",
//                        (String) registro.get("municipio"));
            } else {
                // CREAR nuevo por estado
                Map<String, Object> nuevo = new LinkedHashMap<>();
                nuevo.put("estado", estado);
                nuevo.put("cantidad_infracciones", registro.get("cantidad_infracciones"));

//                Set<String> provincias = new HashSet<>();
//                provincias.add((String) registro.get("provincia_origen"));
//                nuevo.put("provincias_involucradas", provincias);
//
//                Set<String> municipios = new HashSet<>();
//                municipios.add((String) registro.get("municipio"));
//                nuevo.put("municipios_involucrados", municipios);
//
//                nuevo.put("consolidado", true);
                registrosConsolidados.put(estado, nuevo);
            }
        }


        // Convertir Sets a listas
        for (Map<String, Object> registro : registrosConsolidados.values()) {
            if (registro.get("provincias_involucradas") instanceof Set) {
                Set<String> provincias = (Set<String>) registro.get("provincias_involucradas");
                registro.put("provincias_involucradas", new ArrayList<>(provincias));
                registro.put("total_provincias", provincias.size());
            }

            if (registro.get("municipios_involucrados") instanceof Set) {
                Set<String> municipios = (Set<String>) registro.get("municipios_involucrados");
                registro.put("total_municipios", municipios.size());
                registro.remove("municipios_involucrados"); // Solo mantener el conteo
            }
        }

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación genérica - identifica automáticamente campos a sumar
     */
    private List<Map<String, Object>> consolidarGenerico(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return datos;
        }

        // Identificar campos de agrupación (no numéricos) y campos a sumar (numéricos)
        Map<String, Object> primerRegistro = datos.get(0);
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();

        for (Map.Entry<String, Object> entrada : primerRegistro.entrySet()) {
            String campo = entrada.getKey();
            Object valor = entrada.getValue();

            if ("provincia_origen".equals(campo)) {
                continue; // Campo especial
            }

            if (esNumerico(valor)) {
                camposNumericos.add(campo);
            } else {
                camposAgrupacion.add(campo);
            }
        }

        log.info("Consolidación genérica - Campos agrupación: {}, Campos numéricos: {}",
                camposAgrupacion.size(), camposNumericos.size());

        if (camposAgrupacion.isEmpty()) {
            // No hay campos de agrupación, sumar todo
            return sumarTodosLosRegistros(datos, camposNumericos);
        }

        // Consolidar por campos de agrupación
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String clave = crearClaveConsolidacion(registro, camposAgrupacion);

            if (registrosConsolidados.containsKey(clave)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);

                // Sumar todos los campos numéricos
                for (String campo : camposNumericos) {
                    sumarCampoNumerico(registroExistente, registro, campo);
                }

                agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                Set<String> provincias = new HashSet<>();
                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
                nuevoRegistro.put("consolidado", true);
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        log.info("Consolidación genérica completada: {} registros de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    // =============== MÉTODOS UTILITARIOS ===============

    /**
     * Crea una clave de consolidación basada en campos específicos
     */
    private String crearClaveConsolidacion(Map<String, Object> registro, List<String> campos) {
        return campos.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * Suma todos los valores numéricos de un registro a otro
     */
    private void sumarValoresNumericos(Map<String, Object> destino, Map<String, Object> origen) {
        for (Map.Entry<String, Object> entrada : origen.entrySet()) {
            String campo = entrada.getKey();
            Object valor = entrada.getValue();

            if (esNumerico(valor)) {
                sumarCampoNumerico(destino, origen, campo);
            }
        }
    }

    /**
     * Suma un campo numérico específico
     */
    private void sumarCampoNumerico(Map<String, Object> destino, Map<String, Object> origen, String campo) {
        Object valorDestino = destino.get(campo);
        Object valorOrigen = origen.get(campo);

        if (valorDestino == null) valorDestino = 0;
        if (valorOrigen == null) valorOrigen = 0;

        if (esNumerico(valorDestino) && esNumerico(valorOrigen)) {
            Number numeroDestino = (Number) valorDestino;
            Number numeroOrigen = (Number) valorOrigen;

            // Mantener precisión decimal si existe
            if (numeroDestino instanceof Double || numeroOrigen instanceof Double ||
                    numeroDestino instanceof Float || numeroOrigen instanceof Float) {
                destino.put(campo, numeroDestino.doubleValue() + numeroOrigen.doubleValue());
            } else {
                destino.put(campo, numeroDestino.longValue() + numeroOrigen.longValue());
            }
        }
    }

    /**
     * Agrega una provincia al conjunto de provincias origen
     */
    @SuppressWarnings("unchecked")
    private void agregarProvinciaOrigen(Map<String, Object> registro, String provincia) {
        Object provinciasObj = registro.get("provincias_origen");
        Set<String> provincias;

        if (provinciasObj instanceof Set) {
            provincias = (Set<String>) provinciasObj;
        } else {
            provincias = new HashSet<>();
            if (provinciasObj != null) {
                provincias.add(String.valueOf(provinciasObj));
            }
        }

        if (provincia != null) {
            provincias.add(provincia);
        }

        registro.put("provincias_origen", provincias);
    }

    /**
     * Determina si un valor es numérico
     */
    private boolean esNumerico(Object valor) {
        return valor instanceof Number;
    }

    /**
     * Suma todos los registros cuando no hay agrupación
     */
    private List<Map<String, Object>> sumarTodosLosRegistros(List<Map<String, Object>> datos, List<String> camposNumericos) {
        if (datos.isEmpty()) {
            return datos;
        }

        Map<String, Object> registroConsolidado = new LinkedHashMap<>(datos.get(0));
        Set<String> todasLasProvincias = new HashSet<>();

        // Inicializar valores numéricos en 0
        for (String campo : camposNumericos) {
            registroConsolidado.put(campo, 0L);
        }

        // Sumar todos los registros
        for (Map<String, Object> registro : datos) {
            for (String campo : camposNumericos) {
                sumarCampoNumerico(registroConsolidado, registro, campo);
            }

            String provincia = (String) registro.get("provincia_origen");
            if (provincia != null) {
                todasLasProvincias.add(provincia);
            }
        }

        registroConsolidado.put("provincias_origen", todasLasProvincias);
        registroConsolidado.put("consolidado", true);
        registroConsolidado.put("total_registros_originales", datos.size());

        return Arrays.asList(registroConsolidado);
    }

    /**
     * Agrega metadata de consolidación a los registros
     */
    private void agregarMetadataConsolidacion(List<Map<String, Object>> datos, List<InfraccionesRepositoryImpl> repositories) {
        if (datos.isEmpty()) {
            return;
        }

        Set<String> provinciasConsultadas = repositories.stream()
                .map(InfraccionesRepositoryImpl::getProvincia)
                .collect(Collectors.toSet());

        // Agregar metadata al primer registro
        Map<String, Object> primerRegistro = datos.get(0);
        Map<String, Object> consolidacionMetadata = new HashMap<>();
        consolidacionMetadata.put("total_registros_consolidados", datos.size());
        consolidacionMetadata.put("provincias_consultadas", provinciasConsultadas);
        consolidacionMetadata.put("total_provincias", provinciasConsultadas.size());
        consolidacionMetadata.put("fecha_consolidacion", new Date());
        consolidacionMetadata.put("tipo_consolidacion", "agregacion_numerica");

        primerRegistro.put("consolidacion_metadata", consolidacionMetadata);


        log.info("Metadata de consolidación agregada: {} registros finales de {} provincias",
                datos.size(), provinciasConsultadas.size());
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

    @SuppressWarnings("unchecked")
    private void agregarAConjunto(Map<String, Object> registro, String campo, String elemento) {
        Object conjuntoObj = registro.get(campo);
        Set<String> conjunto = (conjuntoObj instanceof Set) ?
                (Set<String>) conjuntoObj : new HashSet<>();

        if (elemento != null && !elemento.trim().isEmpty()) {
            conjunto.add(elemento);
        }

        registro.put(campo, conjunto);
    }

    // =============== MÉTODOS PÚBLICOS ADICIONALES (mantener compatibilidad) ===============

    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        if (!filtros.esConsolidado()) {
            return false;
        }

        Map<String, ?> repositories = repositoryFactory.getAllRepositories();
        if (repositories.isEmpty()) {
            log.warn("No hay repositorios disponibles para consolidación");
            return false;
        }

        log.debug("Consolidación validada: {} repositorios disponibles", repositories.size());
        return true;
    }

    public Map<String, Object> obtenerInfoConsolidacion() {
        Map<String, Object> info = new HashMap<>();

        Set<String> provinciasDisponibles = repositoryFactory.getProvinciasSoportadas();
        Map<String, ?> repositories = repositoryFactory.getAllRepositories();

        info.put("provincias_disponibles", new ArrayList<>(provinciasDisponibles));
        info.put("total_provincias", provinciasDisponibles.size());
        info.put("repositories_activos", repositories.size());
        info.put("consolidacion_habilitada", !repositories.isEmpty());
        info.put("tipo_consolidacion", "agregacion_numerica");

        return info;
    }

    public Map<String, Object> generarResumenConsolidacion(List<Map<String, Object>> datosConsolidados) {
        Map<String, Object> resumen = new HashMap<>();

        if (datosConsolidados == null || datosConsolidados.isEmpty()) {
            resumen.put("total_registros", 0);
            resumen.put("provincias", Collections.emptyList());
            return resumen;
        }

        // Contar registros totales
        long totalRegistros = datosConsolidados.size();

        // Obtener provincias únicas de todos los registros consolidados
        Set<String> todasLasProvincias = new HashSet<>();
        for (Map<String, Object> registro : datosConsolidados) {
            Object provinciasObj = registro.get("provincias_origen");
            if (provinciasObj instanceof Set) {
                todasLasProvincias.addAll((Set<String>) provinciasObj);
            } else if (provinciasObj != null) {
                todasLasProvincias.add(String.valueOf(provinciasObj));
            }
        }

        resumen.put("total_registros_consolidados", totalRegistros);
        resumen.put("total_provincias_involucradas", todasLasProvincias.size());
        resumen.put("provincias", new ArrayList<>(todasLasProvincias));
        resumen.put("fecha_resumen", new Date());
        resumen.put("tipo_consolidacion", "agregacion_numerica");

        log.debug("Resumen de consolidación generado: {} registros consolidados, {} provincias involucradas",
                totalRegistros, todasLasProvincias.size());

        return resumen;
    }
}