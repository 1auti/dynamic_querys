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
     * Mapeo de códigos internos a nombres de provincias normalizados
     */
    private static final Map<String, String> MAPEO_PROVINCIAS = new HashMap<>();

    static {
        MAPEO_PROVINCIAS.put("Buenos Aires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("BuenosAires", "Buenos Aires");
        MAPEO_PROVINCIAS.put("LaPampa", "La Pampa");
        MAPEO_PROVINCIAS.put("Chaco", "Chaco");
        MAPEO_PROVINCIAS.put("EntreRos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Entre Ríos", "Entre Ríos");
        MAPEO_PROVINCIAS.put("Formosa", "Formosa");
    }

    /**
     * Método principal MEJORADO para consolidar datos con orden específico
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación para {} provincias", repositories.size());

        // 1. Recopilar datos de todas las provincias
        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);

        if (todosLosDatos.isEmpty()) {
            log.info("No hay datos para consolidar");
            return Collections.emptyList();
        }

        log.info("Total datos recopilados: {} registros", todosLosDatos.size());

        // 2. Normalizar provincias en todos los registros
        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);

        // 3. PROCESAR CONSOLIDACIÓN POR ORDEN
        List<Map<String, Object>> datosConsolidados = procesarConsolidacionPorOrden(todosLosDatos, filtros);

        // 4. Aplicar límites de paginación
        return aplicarLimites(datosConsolidados, filtros);
    }

    // =============== CONSOLIDACIÓN POR ORDEN MEJORADA ===============

    /**
     * Procesa la consolidación según el orden especificado en 'consolidacion'
     */
    private List<Map<String, Object>> procesarConsolidacionPorOrden(
            List<Map<String, Object>> datos, ParametrosFiltrosDTO filtros) {

        List<String> ordenConsolidacion = filtros.getConsolidacionSeguro();

        if (ordenConsolidacion.isEmpty()) {
            log.info("No hay orden de consolidación especificado - usando consolidación automática por provincia");
            return consolidarAutomaticoPorProvincia(datos);
        }

        log.info("Aplicando consolidación por orden: {}", ordenConsolidacion);

        // CASO ESPECIAL: Solo provincia
        if (ordenConsolidacion.size() == 1
                && ("provincia".equals(ordenConsolidacion.get(0))
                || "contexto".equals(ordenConsolidacion.get(0))
                || "municipio".equals(ordenConsolidacion.get(0)))) {

            log.info("Consolidación SOLO por {} - detectando todos los campos numéricos", ordenConsolidacion.get(0));
            return consolidarSoloPorProvincia(datos);
        }


        // Separar campos de agrupación y campos numéricos según su posición
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();

        for (String campo : ordenConsolidacion) {
            if (esNumericoEnDatos(campo, datos)) {
                camposNumericos.add(campo);
            } else {
                camposAgrupacion.add(campo);
            }
        }

        log.info("Campos de agrupación identificados: {}", camposAgrupacion);
        log.info("Campos numéricos identificados: {}", camposNumericos);

        // Si no hay campos de agrupación, usar provincia por defecto
        if (camposAgrupacion.isEmpty()) {
            camposAgrupacion.add("provincia");
        }

        return consolidarPorCamposEspecificos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * NUEVO: Consolidación específica solo por provincia con todos los campos numéricos
     */
    private List<Map<String, Object>> consolidarSoloPorProvincia(List<Map<String, Object>> datos) {
        log.info("Ejecutando consolidación SOLO por provincia");

        // 1. Detectar TODOS los campos numéricos disponibles
        List<String> todosLosCamposNumericos = detectarTodosLosCamposNumericos(datos);
        log.info("Campos numéricos detectados para sumar: {}", todosLosCamposNumericos);

        // 2. Campos de agrupación: solo provincia
        List<String> camposAgrupacion = Arrays.asList("provincia");

        // 3. Consolidar usando TODOS los campos numéricos
        return consolidarPorCamposEspecificosConTotales(datos, camposAgrupacion, todosLosCamposNumericos);
    }

    /**
     * NUEVO: Detecta TODOS los campos numéricos en los datos
     */
    private List<String> detectarTodosLosCamposNumericos(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return new ArrayList<>();
        }

        // Obtener todos los campos del primer registro
        Set<String> todosCampos = datos.get(0).keySet();
        List<String> camposNumericos = new ArrayList<>();

        // Analizar cada campo
        for (String campo : todosCampos) {
            // Excluir campos que sabemos que no son numéricos
            if (esCampoNoNumerico(campo)) {
                continue;
            }

            // Verificar si el campo es numérico analizando una muestra
            if (esNumericoEnDatosMejorado(campo, datos)) {
                camposNumericos.add(campo);
                log.debug("Campo numérico detectado: {}", campo);
            }
        }

        log.info("Total campos numéricos detectados: {} de {} campos totales",
                camposNumericos.size(), todosCampos.size());

        return camposNumericos;
    }

    /**
     * NUEVO: Identifica campos que definitivamente no son numéricos
     */
    private static final Set<String> CAMPOS_NO_NUMERICOS;

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
        CAMPOS_NO_NUMERICOS = Collections.unmodifiableSet(campos);
    }

    private boolean esCampoNoNumerico(String campo) {
        return CAMPOS_NO_NUMERICOS.contains(campo.toLowerCase());
    }


    /**
     * MEJORADO: Verifica si un campo es numérico con mejor detección
     */
    private boolean esNumericoEnDatosMejorado(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return false;

        int contadorNumerico = 0;
        int contadorNoNulo = 0;
        int muestra = Math.min(50, datos.size()); // Muestra más grande

        for (int i = 0; i < muestra; i++) {
            Object valor = datos.get(i).get(campo);

            if (valor != null) {
                contadorNoNulo++;

                if (esNumerico(valor)) {
                    contadorNumerico++;
                } else if (esNumericoComoString(valor)) {
                    // Intentar parsear strings que podrían ser números
                    contadorNumerico++;
                }
            }
        }

        // Requiere al menos 3 valores no nulos y >80% numéricos
        if (contadorNoNulo < 3) return false;

        double porcentajeNumerico = (double) contadorNumerico / contadorNoNulo;
        boolean esNumericoField = porcentajeNumerico > 0.8;

        if (esNumericoField) {
            log.debug("Campo '{}' es numérico: {}/{} valores ({}%)",
                    campo, contadorNumerico, contadorNoNulo,
                    String.format("%.1f", porcentajeNumerico * 100));
        }

        return esNumericoField;
    }

    /**
     * NUEVO: Verifica si un string puede ser parseado como número
     */
    private boolean esNumericoComoString(Object valor) {
        if (!(valor instanceof String)) return false;

        String str = valor.toString().trim();
        if (str.isEmpty()) return false;

        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * NUEVO: Consolidación con totales mejorada
     */
    private List<Map<String, Object>> consolidarPorCamposEspecificosConTotales(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();
        Map<String, Long> totalesGenerales = new HashMap<>(); // Para tracking de totales

        log.info("Iniciando consolidación con {} campos de agrupación y {} campos numéricos",
                camposAgrupacion.size(), camposNumericos.size());

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupoConsolidado = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoCompleto(registro, camposAgrupacion, camposNumericos));

            // Sumar TODOS los campos numéricos especificados
            sumarCamposNumericosConLog(grupoConsolidado, registro, camposNumericos, totalesGenerales);

            // Incrementar contador de registros consolidados
            Integer contador = (Integer) grupoConsolidado.getOrDefault("registros_consolidados", 0);
            grupoConsolidado.put("registros_consolidados", contador + 1);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());

        // Log de resumen
        log.info("Consolidación completada:");
        log.info("- {} grupos consolidados", resultado.size());
        log.info("- {} registros originales procesados", datos.size());
        log.info("- Totales generales por campo:");

        totalesGenerales.forEach((campo, total) ->
                log.info("  * {}: {}", campo, total));

        return resultado;
    }

    /**
     * NUEVO: Crea un grupo con todos los campos necesarios
     */
    private Map<String, Object> crearGrupoCompleto(Map<String, Object> registro,
                                                   List<String> camposAgrupacion,
                                                   List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Campos de agrupación (mantener valores originales)
        for (String campo : camposAgrupacion) {
            Object valor = registro.get(campo);
            grupo.put(campo, valor);
            log.trace("Agrupación - {}: {}", campo, valor);
        }

        // Campos numéricos (inicializar en 0)
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
            log.trace("Numérico inicializado - {}: 0", campo);
        }

        // Metadatos
        grupo.put("registros_consolidados", 0);

        return grupo;
    }

    /**
     * NUEVO: Suma campos numéricos con logging detallado
     */
    private void sumarCamposNumericosConLog(Map<String, Object> consolidado,
                                            Map<String, Object> registro,
                                            List<String> camposNumericos,
                                            Map<String, Long> totalesGenerales) {

        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);

            if (valorRegistro != null && (esNumerico(valorRegistro) || esNumericoComoString(valorRegistro))) {

                Long valorNumerico = convertirANumero(valorRegistro);

                if (valorNumerico != null) {
                    Long valorActual = 0L;
                    Object valorConsolidado = consolidado.get(campo);
                    if (valorConsolidado != null && esNumerico(valorConsolidado)) {
                        valorActual = ((Number) valorConsolidado).longValue();
                    }

                    Long nuevoTotal = valorActual + valorNumerico;
                    consolidado.put(campo, nuevoTotal);

                    // Actualizar totales generales
                    totalesGenerales.merge(campo, valorNumerico, Long::sum);

                    log.trace("Sumando '{}': {} + {} = {}", campo, valorActual, valorNumerico, nuevoTotal);
                }
            } else {
                log.trace("Campo '{}' no sumable en registro: valor={} (tipo: {})",
                        campo, valorRegistro,
                        valorRegistro != null ? valorRegistro.getClass().getSimpleName() : "null");
            }
        }
    }

    /**
     * NUEVO: Convierte un valor a número de manera segura
     */
    private Long convertirANumero(Object valor) {
        try {
            if (valor instanceof Number) {
                return ((Number) valor).longValue();
            } else if (valor instanceof String) {
                String str = valor.toString().trim();
                if (!str.isEmpty()) {
                    return Math.round(Double.parseDouble(str));
                }
            }
        } catch (Exception e) {
            log.trace("No se pudo convertir a número: {} ({})", valor, e.getMessage());
        }
        return null;
    }

    /**
     * Consolidación automática mejorada cuando no hay orden específico
     */
    private List<Map<String, Object>> consolidarAutomaticoPorProvincia(List<Map<String, Object>> datos) {
        log.info("Ejecutando consolidación automática por provincia");

        // Usar la nueva lógica mejorada
        List<String> camposAgrupacion = Arrays.asList("provincia");
        List<String> todosLosCamposNumericos = detectarTodosLosCamposNumericos(datos);

        return consolidarPorCamposEspecificosConTotales(datos, camposAgrupacion, todosLosCamposNumericos);
    }

    /**
     * Consolida usando campos específicos de agrupación y numéricos (método original)
     */
    private List<Map<String, Object>> consolidarPorCamposEspecificos(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupoConsolidado = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoSimple(registro, camposAgrupacion, camposNumericos));

            // Sumar SOLO los campos numéricos especificados
            sumarCamposNumericos(grupoConsolidado, registro, camposNumericos);

            // Incrementar contador
            Integer contador = (Integer) grupoConsolidado.getOrDefault("registros_consolidados", 0);
            grupoConsolidado.put("registros_consolidados", contador + 1);
        }

        List<Map<String, Object>> resultado = new ArrayList<>(grupos.values());

        log.info("Consolidación por orden completada: {} grupos consolidados", resultado.size());

        return resultado;
    }

    /**
     * Crea un grupo simple con solo los campos especificados (método original)
     */
    private Map<String, Object> crearGrupoSimple(Map<String, Object> registro,
                                                 List<String> camposAgrupacion,
                                                 List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Campos de agrupación (mantener valores originales)
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Campos numéricos (inicializar en 0)
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    /**
     * Suma solo los campos numéricos especificados (método original)
     */
    private void sumarCamposNumericos(Map<String, Object> consolidado,
                                      Map<String, Object> registro,
                                      List<String> camposNumericos) {

        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);
            if (valorRegistro != null && esNumerico(valorRegistro)) {
                Long valorNumerico = ((Number) valorRegistro).longValue();

                Long valorActual = 0L;
                Object valorConsolidado = consolidado.get(campo);
                if (valorConsolidado != null && esNumerico(valorConsolidado)) {
                    valorActual = ((Number) valorConsolidado).longValue();
                }

                consolidado.put(campo, valorActual + valorNumerico);

                log.trace("Sumando campo {}: {} + {} = {}",
                        campo, valorActual, valorNumerico, valorActual + valorNumerico);
            }
        }
    }

    /**
     * Verifica si un campo es numérico analizando los datos reales (método original)
     */
    private boolean esNumericoEnDatos(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return false;

        int contadorNumerico = 0;
        int muestra = Math.min(20, datos.size());

        for (int i = 0; i < muestra; i++) {
            Object valor = datos.get(i).get(campo);
            if (valor != null && esNumerico(valor)) {
                contadorNumerico++;
            }
        }

        // Considerar numérico si >70% de la muestra son números
        return (double) contadorNumerico / muestra > 0.7;
    }

    /**
     * Detecta campos numéricos comunes automáticamente (método original mejorado)
     */
    private List<String> detectarCamposNumericosComunes(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return new ArrayList<>();

        // Lista ampliada de campos comunes
        List<String> camposComunes = Arrays.asList(
                "total", "vehiculos", "motos", "cantidad", "infracciones", "counter",
                "formato_no_valido", "luz_roja", "senda", "cantidad_infracciones",
                "pre_aprobada", "filtrada_municipio", "sin_datos", "prescrita", "filtrada",
                "aprobada", "pdf_generado", "total_eventos", "total_sin_email",
                "velocidad_radar_fijo", "registros_consolidados"
        );

        return camposComunes.stream()
                .filter(campo -> datos.get(0).containsKey(campo))
                .filter(campo -> esNumericoEnDatos(campo, datos))
                .collect(Collectors.toList());
    }

    // =============== MÉTODOS UTILITARIOS (sin cambios) ===============

    /**
     * Normaliza los nombres de provincias en todos los registros
     */
    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = MAPEO_PROVINCIAS.getOrDefault(provincia, provincia);

            // Actualizar todos los campos relacionados con provincia
            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        log.debug("Provincias normalizadas en {} registros", datos.size());
        return datos;
    }

    /**
     * Obtiene la provincia de un registro desde cualquier campo disponible
     */
    private String obtenerProvinciaDelRegistro(Map<String, Object> registro) {
        // Intentar diferentes campos donde puede estar la provincia
        String[] camposProvincia = {"provincia", "provincia_origen", "contexto"};

        for (String campo : camposProvincia) {
            Object valor = registro.get(campo);
            if (valor != null && !valor.toString().trim().isEmpty()) {
                return valor.toString().trim();
            }
        }

        return "SIN_PROVINCIA";
    }

    /**
     * Recopila datos de todos los repositories disponibles
     */
    private List<Map<String, Object>> recopilarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        List<Map<String, Object>> todosLosDatos = new ArrayList<>();

        for (InfraccionesRepositoryImpl repo : repositories) {
            String provincia = repo.getProvincia();

            try {
                List<Map<String, Object>> datosProvider = repo.ejecutarQueryConFiltros(nombreQuery, filtros);

                if (datosProvider != null && !datosProvider.isEmpty()) {
                    // Agregar provincia de origen a cada registro
                    datosProvider.forEach(registro -> {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                    });
                    todosLosDatos.addAll(datosProvider);

                    log.debug("Provincia {}: {} registros obtenidos", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error obteniendo datos de provincia {}: {}", provincia, e.getMessage());
            }
        }

        log.info("Total recopilado: {} registros de {} provincias",
                todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    /**
     * Crea una clave única para el grupo basada en los campos de agrupación
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * Verifica si un valor es numérico
     */
    private boolean esNumerico(Object valor) {
        return valor instanceof Number;
    }

    /**
     * Aplica límites de paginación a los datos consolidados
     */
    private List<Map<String, Object>> aplicarLimites(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        log.debug("Aplicando límites: limite={}, offset={}, total={}", limite, offset, datos.size());

        // Aplicar offset
        if (offset > 0) {
            if (offset >= datos.size()) {
                return Collections.emptyList();
            }
            datos = datos.subList(offset, datos.size());
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        log.debug("Límites aplicados: {} registros finales", datos.size());
        return datos;
    }

    // =============== MÉTODOS PÚBLICOS DE UTILIDAD ===============

    /**
     * Valida si es posible realizar consolidación
     */
    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        if (!filtros.esConsolidado()) {
            return false;
        }

        Map<String, ?> repositories = repositoryFactory.getAllRepositories();
        boolean valido = !repositories.isEmpty();

        if (!valido) {
            log.warn("No hay repositorios disponibles para consolidación");
        }

        return valido;
    }

    /**
     * Obtiene información sobre el estado de la consolidación
     */
    public Map<String, Object> obtenerInfoConsolidacion() {
        Set<String> provinciasDisponibles = repositoryFactory.getProvinciasSoportadas();
        Map<String, ?> repositories = repositoryFactory.getAllRepositories();

        Map<String, Object> info = new HashMap<>();
        info.put("provincias_disponibles", new ArrayList<>(provinciasDisponibles));
        info.put("total_provincias", provinciasDisponibles.size());
        info.put("repositories_activos", repositories.size());
        info.put("consolidacion_habilitada", !repositories.isEmpty());
        info.put("tipo_consolidacion", "simple_por_orden_con_totales");
        info.put("version", "4.0");

        return info;
    }

    /**
     * Genera respuesta optimizada según el formato solicitado
     */
    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        if (datos == null || datos.isEmpty()) {
            return Collections.emptyMap();
        }

        return datos; // Devolver directamente los datos consolidados
    }
}