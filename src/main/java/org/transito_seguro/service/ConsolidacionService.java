package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.model.ConfiguracionConsolidacion;
import org.transito_seguro.model.TipoColumna;
import org.transito_seguro.model.TipoConsolidacion;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

import static org.transito_seguro.model.TipoConsolidacion.*;

@Slf4j
@Service
public class ConsolidacionService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    /**
     * Consolida datos de forma dinámica analizando automáticamente la estructura
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación DINÁMICA de datos para {} provincias", repositories.size());

        // 1. Recopilar todos los datos
        List<Map<String, Object>> todosLosDatos = recopilarDatosDeTodosLosRepositories(repositories, nombreQuery, filtros);

        if (todosLosDatos.isEmpty()) {
            log.info("No hay datos para consolidar");
            return Collections.emptyList();
        }

        // 2. Analizar estructura automáticamente
        ConfiguracionConsolidacion config = analizarEstructuraDatos(todosLosDatos, nombreQuery);

        log.info("Configuración detectada: Tipo={}, Agrupación={}, Numéricos={}, Únicos={}",
                config.getTipo(), config.getCamposAgrupacion(),
                config.getCamposNumericos(), config.getCamposUnicos());

        // 3. Aplicar consolidación dinámica
        List<Map<String, Object>> datosConsolidados = aplicarConsolidacionDinamica(todosLosDatos, config);

        // 4. Aplicar límites
        return aplicarLimitesConsolidacion(datosConsolidados, filtros);
    }

    // =============== ANÁLISIS AUTOMÁTICO DE ESTRUCTURA ===============

    /**
     * Analiza automáticamente la estructura de los datos para determinar la configuración óptima
     */
    private ConfiguracionConsolidacion analizarEstructuraDatos(List<Map<String, Object>> datos, String nombreQuery) {
        ConfiguracionConsolidacion config = new ConfiguracionConsolidacion();

        if (datos.isEmpty()) {
            return config;
        }

        Map<String, Object> registroMuestra = datos.get(0);

        // Analizar todos los registros para obtener estadísticas
        Map<String, TipoColumna> analisisColumnas = analizarColumnas(datos);

        // Detectar campos de agrupación
        config.setCamposAgrupacion(detectarCamposAgrupacion(analisisColumnas));

        // Detectar campos numéricos
        config.setCamposNumericos(detectarCamposNumericos(analisisColumnas));

        // Detectar campos únicos (para deduplicación)
        config.setCamposUnicos(detectarCamposUnicos(analisisColumnas, datos));

        // Detectar campos de tiempo
        config.setCamposTiempo(detectarCamposTiempo(analisisColumnas));

        // Detectar campos de ubicación
        config.setCamposUbicacion(detectarCamposUbicacion(analisisColumnas));

        // Determinar tipo de consolidación óptimo
        config.setTipo(determinarTipoConsolidacion(config, nombreQuery, datos));

        return config;
    }



    /**
     * Analiza cada columna para determinar sus características
     */
    private Map<String, TipoColumna> analizarColumnas(List<Map<String, Object>> datos) {
        Map<String, TipoColumna> analisis = new HashMap<>();

        if (datos.isEmpty()) return analisis;

        // Inicializar análisis para cada columna
        Set<String> todasLasColumnas = datos.get(0).keySet();
        for (String columna : todasLasColumnas) {
            analisis.put(columna, new TipoColumna());
        }

        // Analizar una muestra representativa (máximo 1000 registros para performance)
        int maxMuestra = Math.min(1000, datos.size());
        for (int i = 0; i < maxMuestra; i++) {
            Map<String, Object> registro = datos.get(i);

            for (Map.Entry<String, Object> entrada : registro.entrySet()) {
                String columna = entrada.getKey();
                Object valor = entrada.getValue();

                TipoColumna tipoCol = analisis.get(columna);
                if (tipoCol == null) continue;

                tipoCol.setTotalRegistros(tipoCol.getTotalRegistros() + 1);

                if (valor != null) {
                    // Agregar a valores únicos
                    tipoCol.getValoresUnicos().add(valor);

                    // Determinar tipo
                    if (esNumerico(valor)) {
                        tipoCol.setEsNumerico(true);
                    } else if (valor instanceof String) {
                        tipoCol.setEsTexto(true);
                        String valorStr = (String) valor;

                        // Detectar si parece fecha
                        if (esFecha(valorStr)) {
                            tipoCol.setEsFecha(true);
                        }

                        // Detectar patrones comunes
                        if (tipoCol.getPatron() == null) {
                            tipoCol.setPatron(detectarPatron(valorStr));
                        }
                    }
                }
            }
        }

        // Determinar si cada columna tiene valores únicos (para deduplicación)
        for (TipoColumna tipoCol : analisis.values()) {
            double porcentajeUnicidad = (double) tipoCol.getValoresUnicos().size() / tipoCol.getTotalRegistros();
            tipoCol.setEsUnico(porcentajeUnicidad > 0.8); // 80% de valores únicos
        }

        return analisis;
    }

    /**
     * Detecta campos de agrupación (ubicación, categorías, etc.)
     */
    private List<String> detectarCamposAgrupacion(Map<String, TipoColumna> analisis) {
        List<String> camposAgrupacion = new ArrayList<>();

        // Palabras clave que indican campos de agrupación
        String[] clavesAgrupacion = {
                "municipio", "provincia", "descripcion", "contexto",
                "tipo_infraccion", "estado", "categoria", "lugar", "partido"
        };

        for (String campo : analisis.keySet()) {
            String campoLower = campo.toLowerCase();
            TipoColumna tipo = analisis.get(campo);

            // Verificar palabras clave
            for (String clave : clavesAgrupacion) {
                if (campoLower.contains(clave) && tipo.isEsTexto() && !tipo.isEsUnico()) {
                    camposAgrupacion.add(campo);
                    break;
                }
            }
        }

        return camposAgrupacion;
    }

    /**
     * Detecta campos numéricos para agregación
     */
    private List<String> detectarCamposNumericos(Map<String, TipoColumna> analisis) {
        List<String> camposNumericos = new ArrayList<>();

        // Palabras clave que indican campos numéricos importantes
        String[] clavesNumericas = {
                "total", "cantidad", "count", "velocidad", "luz_roja", "senda",
                "vehiculos", "motos", "formato_no_valido", "infracciones"
        };

        for (Map.Entry<String, TipoColumna> entrada : analisis.entrySet()) {
            String campo = entrada.getKey();
            TipoColumna tipo = entrada.getValue();
            String campoLower = campo.toLowerCase();

            if (tipo.isEsNumerico()) {
                // Priorizar campos con palabras clave importantes
                boolean esPrioritario = Arrays.stream(clavesNumericas)
                        .anyMatch(campoLower::contains);

                if (esPrioritario || tipo.getValoresUnicos().size() > 10) {
                    // Solo incluir si tiene variedad de valores (no solo 0s y 1s)
                    camposNumericos.add(campo);
                }
            }
        }

        return camposNumericos;
    }

    /**
     * Detecta campos únicos para deduplicación
     */
    private List<String> detectarCamposUnicos(Map<String, TipoColumna> analisis, List<Map<String, Object>> datos) {
        List<String> camposUnicos = new ArrayList<>();

        // Palabras clave que indican identificadores únicos
        String[] clavesUnicas = {"id", "dominio", "numero_doc", "cuit", "serie_equipo"};

        for (Map.Entry<String, TipoColumna> entrada : analisis.entrySet()) {
            String campo = entrada.getKey();
            TipoColumna tipo = entrada.getValue();
            String campoLower = campo.toLowerCase();

            // Verificar palabras clave o alta unicidad
            boolean esId = Arrays.stream(clavesUnicas).anyMatch(campoLower::contains);

            if ((esId || tipo.isEsUnico()) && tipo.getValoresUnicos().size() > 1) {
                camposUnicos.add(campo);
            }
        }

        return camposUnicos;
    }

    /**
     * Detecta campos de tiempo/fecha
     */
    private List<String> detectarCamposTiempo(Map<String, TipoColumna> analisis) {
        List<String> camposTiempo = new ArrayList<>();

        String[] clavesTiempo = {"fecha", "date", "emision", "alta", "modificacion", "timestamp"};

        for (Map.Entry<String, TipoColumna> entrada : analisis.entrySet()) {
            String campo = entrada.getKey();
            TipoColumna tipo = entrada.getValue();
            String campoLower = campo.toLowerCase();

            boolean esFecha = Arrays.stream(clavesTiempo).anyMatch(campoLower::contains) || tipo.isEsFecha();

            if (esFecha) {
                camposTiempo.add(campo);
            }
        }

        return camposTiempo;
    }

    /**
     * Detecta campos de ubicación
     */
    private List<String> detectarCamposUbicacion(Map<String, TipoColumna> analisis) {
        List<String> camposUbicacion = new ArrayList<>();

        String[] clavesUbicacion = {"municipio", "provincia", "contexto", "lugar", "localidad", "partido"};

        for (String campo : analisis.keySet()) {
            String campoLower = campo.toLowerCase();

            if (Arrays.stream(clavesUbicacion).anyMatch(campoLower::contains)) {
                camposUbicacion.add(campo);
            }
        }

        return camposUbicacion;
    }

    /**
     * Determina el tipo de consolidación óptimo
     */
    private TipoConsolidacion determinarTipoConsolidacion(ConfiguracionConsolidacion config,
                                                          String nombreQuery,
                                                          List<Map<String, Object>> datos) {

        // Reglas basadas en el contenido
        if (!config.getCamposUnicos().isEmpty() && nombreQuery.contains("personas_juridicas")) {
            return DEDUPLICACION;
        }

        if (!config.getCamposNumericos().isEmpty() && !config.getCamposAgrupacion().isEmpty()) {
            if (!config.getCamposUbicacion().isEmpty() && !config.getCamposTiempo().isEmpty()) {
                return JERARQUICA;
            } else {
                return AGREGACION;
            }
        }

        if (!config.getCamposUnicos().isEmpty() && !config.getCamposNumericos().isEmpty()) {
            return TipoConsolidacion.COMBINADA;
        }

        return AGREGACION; // Default
    }

    // =============== APLICACIÓN DE CONSOLIDACIÓN DINÁMICA ===============

    /**
     * Aplica la consolidación según la configuración detectada
     */
    private List<Map<String, Object>> aplicarConsolidacionDinamica(List<Map<String, Object>> datos,
                                                                   ConfiguracionConsolidacion config) {

        log.info("Aplicando consolidación dinámica tipo: {} a {} registros",
                config.getTipo(), datos.size());

        switch (config.getTipo()) {
            case DEDUPLICACION:
                return aplicarDeduplicacion(datos, config);
            case AGREGACION:
                return aplicarAgregacion(datos, config);
            case JERARQUICA:
                return aplicarConsolidacionJerarquica(datos, config);
            case COMBINADA:
                return aplicarConsolidacionCombinada(datos, config);
            default:
                return aplicarAgregacion(datos, config); // Fallback
        }
    }

    /**
     * Deduplicación por campos únicos
     */
    private List<Map<String, Object>> aplicarDeduplicacion(List<Map<String, Object>> datos,
                                                           ConfiguracionConsolidacion config) {
        if (config.getCamposUnicos().isEmpty()) {
            log.warn("No hay campos únicos para deduplicación, usando todos los registros");
            return datos;
        }

        Map<String, Map<String, Object>> registrosUnicos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // Crear clave única combinando todos los campos únicos
            String claveUnica = config.getCamposUnicos().stream()
                    .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                    .collect(Collectors.joining("|"));

            if (!registrosUnicos.containsKey(claveUnica)) {
                if (config.isMantenerProvinciasOrigen()) {
                    registro.put("provincias_origen", Collections.singleton(registro.get("provincia_origen")));
                }
                registrosUnicos.put(claveUnica, registro);
            }
        }

        log.info("Deduplicación completada: {} registros únicos de {} originales",
                registrosUnicos.size(), datos.size());

        return new ArrayList<>(registrosUnicos.values());
    }

    /**
     * Agregación por campos de agrupación
     */
    private List<Map<String, Object>> aplicarAgregacion(List<Map<String, Object>> datos,
                                                        ConfiguracionConsolidacion config) {
        if (config.getCamposAgrupacion().isEmpty()) {
            // Sin agrupación, sumar todo
            return sumarTodosLosRegistros(datos, config.getCamposNumericos());
        }

        Map<String, Map<String, Object>> registrosConsolidados = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // Crear clave de agrupación
            String claveAgrupacion = config.getCamposAgrupacion().stream()
                    .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                    .collect(Collectors.joining("|"));

            if (registrosConsolidados.containsKey(claveAgrupacion)) {
                Map<String, Object> registroExistente = registrosConsolidados.get(claveAgrupacion);

                // Sumar campos numéricos
                for (String campoNumerico : config.getCamposNumericos()) {
                    sumarCampoNumerico(registroExistente, registro, campoNumerico);
                }

                if (config.isMantenerProvinciasOrigen()) {
                    agregarProvinciaOrigen(registroExistente, (String) registro.get("provincia_origen"));
                }

            } else {
                Map<String, Object> nuevoRegistro = new LinkedHashMap<>(registro);
                if (config.isMantenerProvinciasOrigen()) {
                    Set<String> provincias = new HashSet<>();
                    provincias.add((String) registro.get("provincia_origen"));
                    nuevoRegistro.put("provincias_origen", provincias);
                }
                registrosConsolidados.put(claveAgrupacion, nuevoRegistro);
            }
        }

        log.info("Agregación completada: {} registros consolidados de {} originales",
                registrosConsolidados.size(), datos.size());

        return new ArrayList<>(registrosConsolidados.values());
    }

    /**
     * Consolidación jerárquica por ubicación y tiempo
     */
    private List<Map<String, Object>> aplicarConsolidacionJerarquica(List<Map<String, Object>> datos,
                                                                     ConfiguracionConsolidacion config) {
        Map<String, Map<String, Object>> consolidadoPorUbicacion = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            // Obtener ubicación (primer campo de ubicación disponible)
            String ubicacion = obtenerValorDeCampos(registro, config.getCamposUbicacion(), "SIN_UBICACION");

            // Obtener período temporal
            String periodo = obtenerValorDeCampos(registro, config.getCamposTiempo(), "SIN_PERIODO");
            if (periodo != null && !periodo.equals("SIN_PERIODO")) {
                periodo = extraerPeriodoDeFecha(periodo);
            }

            // Obtener o crear entrada de ubicación
            Map<String, Object> datosDeUbicacion = consolidadoPorUbicacion.computeIfAbsent(ubicacion, k -> {
                Map<String, Object> nueva = new LinkedHashMap<>();
                nueva.put("ubicacion", ubicacion);
                nueva.put("total_registros_consolidados", 0);
                nueva.put("desglose_temporal", new LinkedHashMap<String, Object>());
                nueva.put("total_general", 0L);
                return nueva;
            });

            @SuppressWarnings("unchecked")
            Map<String, Object> desgloseTemporal = (Map<String, Object>) datosDeUbicacion.get("desglose_temporal");

            // Consolidar por período
            for (String campoNumerico : config.getCamposNumericos()) {
                Object valor = registro.get(campoNumerico);
                if (valor != null && esNumerico(valor)) {
                    Long valorNumerico = ((Number) valor).longValue();

                    // Sumar al período específico
                    Long valorPeriodo = (Long) desgloseTemporal.getOrDefault(periodo, 0L);
                    desgloseTemporal.put(periodo, valorPeriodo + valorNumerico);

                    // Sumar al total general
                    Long totalGeneral = (Long) datosDeUbicacion.get("total_general");
                    datosDeUbicacion.put("total_general", totalGeneral + valorNumerico);
                }
            }

            // Incrementar contador
            int registrosConsolidados = (Integer) datosDeUbicacion.get("total_registros_consolidados");
            datosDeUbicacion.put("total_registros_consolidados", registrosConsolidados + 1);
        }

        // Generar resultado final
        List<Map<String, Object>> resultado = new ArrayList<>();
        for (Map<String, Object> datosUbicacion : consolidadoPorUbicacion.values()) {
            Map<String, Object> registroConsolidado = new LinkedHashMap<>();
            registroConsolidado.put("ubicacion", datosUbicacion.get("ubicacion"));
            registroConsolidado.put("total_infracciones", datosUbicacion.get("total_general"));
            registroConsolidado.put("desglose_temporal", datosUbicacion.get("desglose_temporal"));
            resultado.add(registroConsolidado);
        }

        log.info("Consolidación jerárquica completada: {} ubicaciones consolidadas de {} registros originales",
                resultado.size(), datos.size());

        return resultado;
    }

    /**
     * Consolidación combinada (deduplicación + agregación)
     */
    private List<Map<String, Object>> aplicarConsolidacionCombinada(List<Map<String, Object>> datos,
                                                                    ConfiguracionConsolidacion config) {
        // Primero deduplicar
        List<Map<String, Object>> datosDedupe = aplicarDeduplicacion(datos, config);

        // Luego agregar
        return aplicarAgregacion(datosDedupe, config);
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private boolean esNumerico(Object valor) {
        return valor instanceof Number;
    }

    private boolean esFecha(String valor) {
        // Patrones comunes de fecha
        return valor.matches("\\d{1,2}[/-]\\d{1,2}[/-]\\d{4}") ||
                valor.matches("\\d{4}-\\d{1,2}-\\d{1,2}") ||
                valor.matches("\\d{4}-\\d{1,2}") ||
                valor.matches("\\d{1,2}/\\d{1,2}/\\d{4}");
    }

    private Pattern detectarPatron(String valor) {
        // Detectar patrones comunes como dominios de vehículos
        if (valor.matches("^[A-Z]{2,3}[0-9]{3}[A-Z]{2}$")) {
            return Pattern.compile("^[A-Z]{2,3}[0-9]{3}[A-Z]{2}$"); // Dominio auto
        }
        if (valor.matches("^[0-9]{3}[A-Z]{3}$")) {
            return Pattern.compile("^[0-9]{3}[A-Z]{3}$"); // Dominio moto
        }
        return null;
    }

    private String obtenerValorDeCampos(Map<String, Object> registro, List<String> campos, String valorDefault) {
        for (String campo : campos) {
            Object valor = registro.get(campo);
            if (valor != null && !valor.toString().trim().isEmpty()) {
                return valor.toString().trim();
            }
        }
        return valorDefault;
    }

    private String extraerPeriodoDeFecha(String fecha) {
        if (fecha == null) return "SIN_PERIODO";

        try {
            // Formato DD/MM/YYYY
            if (fecha.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
                String[] partes = fecha.split("/");
                if (partes.length >= 3) {
                    String mes = partes[1].length() == 1 ? "0" + partes[1] : partes[1];
                    return partes[2] + "-" + mes;
                }
            }
            // Formato YYYY-MM-DD
            if (fecha.matches("\\d{4}-\\d{1,2}-\\d{1,2}")) {
                return fecha.substring(0, 7);
            }
            // Formato YYYY-MM
            if (fecha.matches("\\d{4}-\\d{1,2}")) {
                return fecha;
            }
        } catch (Exception e) {
            log.warn("Error extrayendo período de fecha '{}': {}", fecha, e.getMessage());
        }

        return "SIN_PERIODO";
    }

    @SuppressWarnings("unchecked")
    private void agregarProvinciaOrigen(Map<String, Object> registro, String provincia) {
        Object provinciasObj = registro.get("provincias_origen");
        Set<String> provincias = (provinciasObj instanceof Set) ?
                (Set<String>) provinciasObj : new HashSet<>();

        if (provincia != null) {
            provincias.add(provincia);
        }
        registro.put("provincias_origen", provincias);
    }

    private void sumarCampoNumerico(Map<String, Object> destino, Map<String, Object> origen, String campo) {
        Object valorDestino = destino.get(campo);
        Object valorOrigen = origen.get(campo);

        if (valorDestino == null) valorDestino = 0;
        if (valorOrigen == null) valorOrigen = 0;

        if (esNumerico(valorDestino) && esNumerico(valorOrigen)) {
            Number numeroDestino = (Number) valorDestino;
            Number numeroOrigen = (Number) valorOrigen;
            destino.put(campo, numeroDestino.longValue() + numeroOrigen.longValue());
        }
    }

    private List<Map<String, Object>> sumarTodosLosRegistros(List<Map<String, Object>> datos, List<String> camposNumericos) {
        if (datos.isEmpty()) return datos;

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

    // =============== MÉTODOS AUXILIARES MANTENIDOS ===============

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

    // Métodos públicos mantenidos para compatibilidad
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
        info.put("tipo_consolidacion", "dinamica_adaptativa");

        return info;
    }

    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            return Collections.emptyMap();
        }
        return datos; // Por ahora devolver datos directamente, se puede optimizar según formato
    }
}