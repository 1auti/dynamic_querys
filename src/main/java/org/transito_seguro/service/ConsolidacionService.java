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
            return "personas_juridicas";
        } else if (nombreQuery.contains("reporte_general") || nombreQuery.contains("detallado")) {
            return "reporte_infracciones";
        } else if (nombreQuery.contains("radar_fijo") || columnas.contains("velocidad_radar_fijo")) {
            return "radar_fijo"; // ← DETECCIÓN ESPECÍFICA PARA RADAR FIJO
        } else if (nombreQuery.contains("semaforo") || columnas.contains("luz_roja")) {
            return "reporte_equipos";
        } else if (nombreQuery.contains("vehiculos_municipio")) {
            return "vehiculos_municipio";
        } else if (nombreQuery.contains("sin_email")) {
            return "sin_email";
        } else if (nombreQuery.contains("estado")) {
            return "infracciones_estado";
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
            case "radar_fijo":  // ← NUEVO CASO ESPECÍFICO
                return consolidarReporteRadarFijo(datos);
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
     * Método específico para consolidación de reportes de radar fijo
     */
    private List<Map<String, Object>> consolidarReporteRadarFijo(List<Map<String, Object>> datos) {
        log.info("Aplicando consolidación específica para reporte de radar fijo");

        // Usar la consolidación jerárquica estándar pero con logging específico
        List<Map<String, Object>> resultado = consolidarJerarquico(datos);

        // Agregar metadatos específicos para radar fijo
        for (Map<String, Object> registro : resultado) {
//            registro.put("tipo_reporte", "radar_fijo");
//            registro.put("tipo_consolidacion", "jerarquica_radar_fijo");

            // Calcular estadísticas específicas de radar si están disponibles
            @SuppressWarnings("unchecked")
            Map<String, Object> desgloseTemporal = (Map<String, Object>) registro.get("desglose_temporal");

            if (desgloseTemporal != null) {
                // Encontrar el mes con más infracciones
                String mesMasActivo = desgloseTemporal.entrySet().stream()
                        .max((e1, e2) -> Long.compare(
                                ((Number) e1.getValue()).longValue(),
                                ((Number) e2.getValue()).longValue()))
                        .map(Map.Entry::getKey)
                        .orElse("N/A");

//                registro.put("mes_mas_activo", mesMasActivo);
//                registro.put("valor_mes_mas_activo", desgloseTemporal.get(mesMasActivo));
            }
        }

        return resultado;
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
//                provincias.add((String) registro.get("provincia_origen"));
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
//                provincias.add((String) registro.get("provincia_origen"));
                nuevoRegistro.put("provincias_origen", provincias);
//                nuevoRegistro.put("consolidado", true);
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
    /**
     * Consolidación genérica MEJORADA - crea estructura jerárquica por municipio y período
     */
    private List<Map<String, Object>> consolidarGenerico(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return datos;
        }

        log.info("Iniciando consolidación genérica jerárquica para {} registros", datos.size());

        // Detectar si podemos hacer consolidación jerárquica
        Map<String, Object> primerRegistro = datos.get(0);
        boolean puedeConsolidarJerarquico = detectarConsolidacionJerarquica(primerRegistro);

        if (puedeConsolidarJerarquico) {
            return consolidarJerarquico(datos);
        } else {
            return consolidarGenericoSimple(datos);
        }
    }

    /**
     * Detecta si los datos permiten consolidación jerárquica
     */
    private boolean detectarConsolidacionJerarquica(Map<String, Object> registro) {
        Set<String> campos = registro.keySet();

        // Verificar campos de ubicación (más flexible)
        boolean tieneUbicacion = campos.contains("municipio") ||
                campos.contains("descripcion") ||
                campos.contains("provincia") ||
                campos.contains("contexto");

        // Verificar campos temporales (más formatos)
        boolean tieneTiempo = campos.contains("fecha_emision") ||
                campos.contains("fecha_alta") ||
                campos.contains("fecha");  // ← AGREGAR ESTE CAMPO

        // Verificar campos numéricos (más específicos para radar)
        boolean tieneConteo = campos.contains("cantidad") ||
                campos.contains("count") ||
                campos.contains("total") ||
                campos.contains("velocidad_radar_fijo") ||  // ← ESPECÍFICO PARA RADAR
                campos.contains("luz_roja") ||              // ← ESPECÍFICO PARA SEMÁFOROS
                campos.contains("senda") ||                 // ← ESPECÍFICO PARA SEMÁFOROS
                tieneAlgunCampoNumerico(registro);

        // Verificar si es reporte de equipos (detección específica)
        boolean esReporteEquipos = campos.contains("serie_equipo") ||
                campos.contains("lugar");

        boolean resultado = tieneUbicacion && tieneTiempo && tieneConteo;

        log.debug("Detección consolidación jerárquica: ubicacion={}, tiempo={}, conteo={}, equipos={} -> resultado={}",
                tieneUbicacion, tieneTiempo, tieneConteo, esReporteEquipos, resultado);

        // Log adicional para depuración
        if (!resultado) {
            log.debug("Campos disponibles: {}", campos);
            log.debug("Registro ejemplo: {}", registro);
        }

        return resultado;
    }

    public boolean puedeConsolidarRegistro(Map<String, Object> registro) {
        boolean resultado = detectarConsolidacionJerarquica(registro);

        if (resultado) {
            String municipio = obtenerMunicipio(registro);
            String periodo = obtenerPeriodo(registro);
            List<String> camposNumericos = identificarCamposNumericos(registro);

            log.info("Registro PUEDE ser consolidado:");
            log.info("  - Municipio: {}", municipio);
            log.info("  - Período: {}", periodo);
            log.info("  - Campos numéricos: {}", camposNumericos);
        } else {
            log.warn("Registro NO puede ser consolidado - faltan campos necesarios");
        }

        return resultado;
    }

    /**
     * Consolidación jerárquica que genera estructura anidada por municipio y período
     */
    private List<Map<String, Object>> consolidarJerarquico(List<Map<String, Object>> datos) {

        // Estructura: municipio -> período -> datos consolidados
        Map<String, Map<String, Object>> consolidadoPorMunicipio = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String municipio = obtenerMunicipio(registro);
            String periodo = obtenerPeriodo(registro);
            List<String> camposNumericos = identificarCamposNumericos(registro);

            if (municipio == null || periodo == null || camposNumericos.isEmpty()) {
                log.warn("Registro sin municipio, período o campos numéricos válidos: {}", registro);
                continue;
            }

            // Obtener o crear entrada del municipio
            Map<String, Object> datosDelMunicipio = consolidadoPorMunicipio.computeIfAbsent(municipio,
                    k -> {
                        Map<String, Object> nuevoMunicipio = new LinkedHashMap<>();
                        nuevoMunicipio.put("municipio", municipio);
                        nuevoMunicipio.put("total_registros_consolidados", 0);
                        nuevoMunicipio.put("desglose_temporal", new LinkedHashMap<String, Object>());
                        nuevoMunicipio.put("total_general", 0L);
                        return nuevoMunicipio;
                    });

            @SuppressWarnings("unchecked")
            Map<String, Object> desgloseTemporal = (Map<String, Object>) datosDelMunicipio.get("desglose_temporal");

            // Consolidar por período
            for (String campo : camposNumericos) {
                Object valor = registro.get(campo);
                if (valor != null && esNumerico(valor)) {
                    Long valorNumerico = ((Number) valor).longValue();

                    // Sumar al período específico
                    Long valorPeriodo = (Long) desgloseTemporal.getOrDefault(periodo, 0L);
                    desgloseTemporal.put(periodo, valorPeriodo + valorNumerico);

                    // Sumar al total general
                    Long totalGeneral = (Long) datosDelMunicipio.get("total_general");
                    datosDelMunicipio.put("total_general", totalGeneral + valorNumerico);
                }
            }

            // Incrementar contador de registros consolidados
            int registrosConsolidados = (Integer) datosDelMunicipio.get("total_registros_consolidados");
            datosDelMunicipio.put("total_registros_consolidados", registrosConsolidados + 1);
        }

        // Generar resultado final
        List<Map<String, Object>> resultado = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> entrada : consolidadoPorMunicipio.entrySet()) {
            String municipio = entrada.getKey();
            Map<String, Object> datosMunicipio = entrada.getValue();

            @SuppressWarnings("unchecked")
            Map<String, Object> desgloseTemporal = (Map<String, Object>) datosMunicipio.get("desglose_temporal");

            // Crear registro consolidado
            Map<String, Object> registroConsolidado = new LinkedHashMap<>();
            registroConsolidado.put("municipio", municipio);
//            registroConsolidado.put("provincia", datosMunicipio.get("provincia"));
            registroConsolidado.put("total_infracciones", datosMunicipio.get("total_general"));
            registroConsolidado.put("desglose_temporal", desgloseTemporal);
//            registroConsolidado.put("registros_consolidados", datosMunicipio.get("total_registros_consolidados"));
//            registroConsolidado.put("es_consolidado", true);
//            registroConsolidado.put("tipo_consolidacion", "jerarquica_municipio_periodo");

            // Estadísticas adicionales
//            registroConsolidado.put("total_periodos", desgloseTemporal.size());

            resultado.add(registroConsolidado);
        }

        // Ordenar por provincia y municipio
        resultado.sort((r1, r2) -> {
            String prov1 = (String) r1.get("provincia");
            String prov2 = (String) r2.get("provincia");
            int compProv = (prov1 != null && prov2 != null) ? prov1.compareTo(prov2) : 0;

            if (compProv != 0) return compProv;

            String mun1 = (String) r1.get("municipio");
            String mun2 = (String) r2.get("municipio");
            return (mun1 != null && mun2 != null) ? mun1.compareTo(mun2) : 0;
        });

        log.info("Consolidación jerárquica completada: {} municipios consolidados de {} registros originales",
                resultado.size(), datos.size());

        return resultado;
    }

    /**
     * Genera estructura de respuesta compacta como en el ejemplo deseado
     */
    public Map<String, Object> generarRespuestaJerarquicaCompacta(List<Map<String, Object>> datosConsolidados) {
        Map<String, Object> respuestaCompacta = new LinkedHashMap<>();

        for (Map<String, Object> municipioData : datosConsolidados) {
            String municipio = (String) municipioData.get("municipio");
            Long totalInfracciones = (Long) municipioData.get("total_infracciones");

            @SuppressWarnings("unchecked")
            Map<String, Object> desgloseTemporal = (Map<String, Object>) municipioData.get("desglose_temporal");

            Map<String, Object> datosDelMunicipio = new LinkedHashMap<>();
            datosDelMunicipio.put("total_infracciones", totalInfracciones);
            datosDelMunicipio.put("desglose_mensual", desgloseTemporal);

            respuestaCompacta.put(municipio, datosDelMunicipio);
        }

        return respuestaCompacta;
    }

    /**
     * Consolidación genérica simple (fallback)
     */
    private List<Map<String, Object>> consolidarGenericoSimple(List<Map<String, Object>> datos) {
        // Tu implementación anterior mejorada
        Map<String, Object> primerRegistro = datos.get(0);

        List<String> camposAgrupacionPrioritarios = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();

        // Identificar campos de agrupación prioritarios
        if (primerRegistro.containsKey("provincia_origen")) {
            camposAgrupacionPrioritarios.add("provincia_origen");
        } else if (primerRegistro.containsKey("provincia")) {
            camposAgrupacionPrioritarios.add("provincia");
        }

        if (primerRegistro.containsKey("municipio")) {
            camposAgrupacionPrioritarios.add("municipio");
        } else if (primerRegistro.containsKey("descripcion")) {
            camposAgrupacionPrioritarios.add("descripcion");
        }

        // Identificar campos numéricos
        for (Map.Entry<String, Object> entrada : primerRegistro.entrySet()) {
            if (esNumerico(entrada.getValue())) {
                camposNumericos.add(entrada.getKey());
            }
        }

        // Consolidar por campos de agrupación
        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String clave = crearClaveConsolidacion(registro, camposAgrupacionPrioritarios);

            if (registrosConsolidados.containsKey(clave)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(clave);

                // Sumar campos numéricos
                for (String campo : camposNumericos) {
                    sumarCampoNumerico(registroExistente, registro, campo);
                }

                // Incrementar contador de registros consolidados
                int contadorActual = (Integer) registroExistente.getOrDefault("registros_consolidados", 1);
                registroExistente.put("registros_consolidados", contadorActual + 1);

            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                nuevoRegistro.put("registros_consolidados", 1);
                nuevoRegistro.put("es_consolidado", true);
                nuevoRegistro.put("tipo_consolidacion", "generica_simple");
                registrosConsolidados.put(clave, nuevoRegistro);
            }
        }

        return new ArrayList<>(registrosConsolidados.values());
    }

// =============== MÉTODOS AUXILIARES PARA CONSOLIDACIÓN JERÁRQUICA ===============

    /**
     * Obtiene el municipio del registro
     */
    private String obtenerMunicipio(Map<String, Object> registro) {
        // Probar en orden de prioridad
        String[] camposPosibles = {"municipio", "descripcion", "contexto"};

        for (String campo : camposPosibles) {
            if (registro.containsKey(campo)) {
                String valor = (String) registro.get(campo);
                if (valor != null && !valor.trim().isEmpty()) {
                    return valor.trim();
                }
            }
        }

        return "SIN_MUNICIPIO";
    }

    /**
     * Obtiene el período temporal del registro
     */
    private String obtenerPeriodo(Map<String, Object> registro) {
        // 1. Probar fecha_emision (formato YYYY-MM)
        if (registro.containsKey("fecha_emision")) {
            return (String) registro.get("fecha_emision");
        }

        // 2. Probar campo "fecha" (formato DD/MM/YYYY)
        if (registro.containsKey("fecha")) {
            String fecha = (String) registro.get("fecha");
            return extraerPeriodoDeFecha(fecha);
        }

        // 3. Probar fecha_alta y extraer año-mes
        if (registro.containsKey("fecha_alta")) {
            String fechaAlta = (String) registro.get("fecha_alta");
            return extraerPeriodoDeFecha(fechaAlta);
        }

        return "SIN_PERIODO";
    }

    /**
     * Extrae período de una fecha en varios formatos
     */
    private String extraerPeriodoDeFecha(String fecha) {
        if (fecha == null) return "SIN_PERIODO";

        try {
            // Formato DD/MM/YYYY (como "01/04/2025")
            if (fecha.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] partes = fecha.split("/");
                String dia = partes[0];
                String mes = partes[1];
                String año = partes[2];
                return año + "-" + mes;  // Resultado: "2025-04"
            }

            // Formato DD/MM/YYYY con separador diferente
            if (fecha.matches("\\d{1,2}[/-]\\d{1,2}[/-]\\d{4}")) {
                String[] partes = fecha.split("[/-]");
                if (partes.length >= 3) {
                    String mes = partes[1].length() == 1 ? "0" + partes[1] : partes[1];
                    return partes[2] + "-" + mes;
                }
            }

            // Formato YYYY-MM-DD
            if (fecha.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return fecha.substring(0, 7); // YYYY-MM
            }

            // Formato YYYY-MM ya correcto
            if (fecha.matches("\\d{4}-\\d{2}")) {
                return fecha;
            }

        } catch (Exception e) {
            log.warn("Error extrayendo período de fecha '{}': {}", fecha, e.getMessage());
        }

        return "SIN_PERIODO";
    }


    /**
     * Identifica campos numéricos del registro
     */
    private List<String> identificarCamposNumericos(Map<String, Object> registro) {
        List<String> camposNumericos = new ArrayList<>();

        // Campos específicos prioritarios para consolidación
        String[] camposPrioritarios = {
                "velocidad_radar_fijo", "luz_roja", "senda", "total",
                "cantidad", "count", "vehiculos", "motos", "formato_no_valido"
        };

        // Primero agregar campos prioritarios si existen y son numéricos
        for (String campo : camposPrioritarios) {
            if (registro.containsKey(campo) && esNumerico(registro.get(campo))) {
                camposNumericos.add(campo);
            }
        }

        // Luego agregar otros campos numéricos no prioritarios
        for (Map.Entry<String, Object> entrada : registro.entrySet()) {
            String campo = entrada.getKey();
            Object valor = entrada.getValue();

            // Saltar si ya está en prioritarios
            if (Arrays.asList(camposPrioritarios).contains(campo)) {
                continue;
            }

            // Saltar campos de metadatos
            if (campo.equals("provincia_origen") || campo.equals("es_consolidado") ||
                    campo.contains("metadata") || campo.equals("id") ||
                    campo.contains("provincia") || campo.equals("consolidado")) {
                continue;
            }

            if (esNumerico(valor)) {
                camposNumericos.add(campo);
            }
        }

        log.debug("Campos numéricos identificados: {}", camposNumericos);
        return camposNumericos;
    }

    /**
     * Verifica si el registro tiene algún campo numérico
     */
    private boolean tieneAlgunCampoNumerico(Map<String, Object> registro) {
        return registro.values().stream().anyMatch(this::esNumerico);
    }

    /**
     * Método público para obtener respuesta en formato compacto deseado
     */
    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            return Collections.emptyMap();
        }

        // Verificar si es consolidación jerárquica
        boolean esJerarquica = datos.stream()
                .anyMatch(r -> "jerarquica_municipio_periodo".equals(r.get("tipo_consolidacion")));

        if (esJerarquica && "json".equalsIgnoreCase(formato)) {
            // Generar respuesta compacta como en el ejemplo deseado
            return generarRespuestaJerarquicaCompacta(datos);
        } else {
            // Respuesta estándar
            return datos;
        }
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