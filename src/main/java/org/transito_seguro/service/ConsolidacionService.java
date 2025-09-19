package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.factory.RepositoryFactory;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.utils.CamposNoNumericos;
import org.transito_seguro.utils.NormalizadorProvincias;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConsolidacionService {

    @Autowired
    private RepositoryFactory repositoryFactory;

    // Constantes para evitar magic numbers
    private static final int MIN_MUESTRA_NUMERICA = 3;
    private static final double UMBRAL_NUMERICO = 0.8;
    private static final int TAMAÑO_MUESTRA = 50;

    /**
     * Método principal para consolidar datos
     */
    public List<Map<String, Object>> consolidarDatos(
            List<InfraccionesRepositoryImpl> repositories,
            String nombreQuery,
            ParametrosFiltrosDTO filtros) {

        log.info("Iniciando consolidación para {} provincias", repositories.size());

        List<Map<String, Object>> todosLosDatos = recopilarDatos(repositories, nombreQuery, filtros);
        if (todosLosDatos.isEmpty()) {
            return Collections.emptyList();
        }

        todosLosDatos = normalizarProvinciasEnDatos(todosLosDatos);
        List<Map<String, Object>> datosConsolidados = procesarConsolidacion(todosLosDatos, filtros);

        return aplicarLimites(datosConsolidados, filtros);
    }

    /**
     * Procesamiento simplificado de consolidación
     */
    private List<Map<String, Object>> procesarConsolidacion(
            List<Map<String, Object>> datos, ParametrosFiltrosDTO filtros) {

        List<String> ordenConsolidacion = filtros.getConsolidacionSeguro();

        if (ordenConsolidacion.isEmpty()) {
            return consolidarPorProvincia(datos);
        }

        // Si solo hay un campo y es provincia, usar consolidación completa
        // Si solo hay un campo y es un campo de agrupación (como provincia), usar consolidación completa
        if (ordenConsolidacion.size() == 1 &&
                CamposNoNumericos.esNoNumerico(ordenConsolidacion.get(0))) {
            return consolidarPorProvincia(datos);
        }
        // Separar campos de agrupación vs numéricos
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();

        for (String campo : ordenConsolidacion) {
            if (esNumericoEnDatos(campo, datos)) {
                camposNumericos.add(campo);
            } else {
                camposAgrupacion.add(campo);
            }
        }

        if (camposAgrupacion.isEmpty()) {
            camposAgrupacion.add("provincia");
        }

        log.info("Consolidando por: {} (agrupación) + {} (numéricos)",
                camposAgrupacion, camposNumericos);

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Consolidación específica por provincia detectando todos los campos numéricos
     */
    private List<Map<String, Object>> consolidarPorProvincia(List<Map<String, Object>> datos) {
        log.info("Consolidación por provincia - detectando campos numéricos automáticamente");

        List<String> camposNumericos = detectarCamposNumericos(datos);
        List<String> camposAgrupacion = Arrays.asList("provincia");

        return consolidarPorCampos(datos, camposAgrupacion, camposNumericos);
    }

    /**
     * Detecta automáticamente campos numéricos
     */
    private List<String> detectarCamposNumericos(List<Map<String, Object>> datos) {
        if (datos.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> todosCampos = datos.get(0).keySet();
        List<String> camposNumericos = new ArrayList<>();

        for (String campo : todosCampos) {
            // Si el campo NO está en la lista de no-numéricos Y es numérico en los datos
            if (!CamposNoNumericos.esNoNumerico(campo) &&
                    esNumericoEnDatos(campo, datos)) {
                camposNumericos.add(campo);
            }
        }

        log.info("Campos numéricos detectados: {} de {} totales",
                camposNumericos.size(), todosCampos.size());

        return camposNumericos;
    }

    /**
     * Consolidación por campos específicos - método unificado
     */
    private List<Map<String, Object>> consolidarPorCampos(
            List<Map<String, Object>> datos,
            List<String> camposAgrupacion,
            List<String> camposNumericos) {

        Map<String, Map<String, Object>> grupos = new LinkedHashMap<>();

        for (Map<String, Object> registro : datos) {
            String claveGrupo = crearClaveGrupo(registro, camposAgrupacion);

            Map<String, Object> grupo = grupos.computeIfAbsent(claveGrupo,
                    k -> crearGrupoNuevo(registro, camposAgrupacion, camposNumericos));

            sumarCamposNumericos(grupo, registro, camposNumericos);
        }

        return new ArrayList<>(grupos.values());
    }

    /**
     * Crea un nuevo grupo inicializado
     */
    private Map<String, Object> crearGrupoNuevo(Map<String, Object> registro,
                                                List<String> camposAgrupacion,
                                                List<String> camposNumericos) {
        Map<String, Object> grupo = new LinkedHashMap<>();

        // Copiar campos de agrupación
        for (String campo : camposAgrupacion) {
            grupo.put(campo, registro.get(campo));
        }

        // Inicializar campos numéricos en 0
        for (String campo : camposNumericos) {
            grupo.put(campo, 0L);
        }

        return grupo;
    }

    /**
     * Suma valores numéricos - método simplificado
     */
    private void sumarCamposNumericos(Map<String, Object> grupo,
                                      Map<String, Object> registro,
                                      List<String> camposNumericos) {
        for (String campo : camposNumericos) {
            Object valorRegistro = registro.get(campo);

            if (valorRegistro == null) continue;

            Long valorNumerico = convertirANumero(valorRegistro);
            if (valorNumerico == null) continue;

            Long valorActual = obtenerValorNumerico(grupo.get(campo));
            grupo.put(campo, valorActual + valorNumerico);
        }
    }

    /**
     * Obtiene valor numérico de forma segura
     */
    private Long obtenerValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }
        return 0L;
    }

    /**
     * Convierte objeto a número de forma segura
     */
    private Long convertirANumero(Object valor) {
        if (valor instanceof Number) {
            return ((Number) valor).longValue();
        }

        if (valor instanceof String) {
            String str = valor.toString().trim();
            if (!str.isEmpty()) {
                try {
                    return Math.round(Double.parseDouble(str));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }

        return null;
    }

    /**
     * Verifica si un campo es numérico analizando los datos
     */
    private boolean esNumericoEnDatos(String campo, List<Map<String, Object>> datos) {
        if (datos.isEmpty()) return false;

        int contadorNumerico = 0;
        int contadorNoNulo = 0;
        int muestra = Math.min(TAMAÑO_MUESTRA, datos.size());

        for (int i = 0; i < muestra; i++) {
            Object valor = datos.get(i).get(campo);
            if (valor != null) {
                contadorNoNulo++;
                if (esValorNumerico(valor)) {
                    contadorNumerico++;
                }
            }
        }

        if (contadorNoNulo < MIN_MUESTRA_NUMERICA) return false;

        return (double) contadorNumerico / contadorNoNulo > UMBRAL_NUMERICO;
    }

    /**
     * Verifica si un valor específico es numérico
     */
    private boolean esValorNumerico(Object valor) {
        if (valor instanceof Number) {
            return true;
        }

        if (valor instanceof String) {
            try {
                Double.parseDouble(valor.toString().trim());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return false;
    }

    // =============== MÉTODOS UTILITARIOS ===============

    /**
     * Normaliza provincias en los datos
     */
    private List<Map<String, Object>> normalizarProvinciasEnDatos(List<Map<String, Object>> datos) {
        for (Map<String, Object> registro : datos) {
            String provincia = obtenerProvinciaDelRegistro(registro);
            String provinciaNormalizada = NormalizadorProvincias.normalizar(provincia);

            registro.put("provincia", provinciaNormalizada);
            registro.put("provincia_origen", provinciaNormalizada);
        }

        return datos;
    }

    /**
     * Obtiene provincia de un registro
     */
    private String obtenerProvinciaDelRegistro(Map<String, Object> registro) {
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
     * Recopila datos de todos los repositories
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
                    // Agregar información de provincia a cada registro
                    for (Map<String, Object> registro : datosProvider) {
                        registro.put("provincia", provincia);
                        registro.put("provincia_origen", provincia);
                    }

                    todosLosDatos.addAll(datosProvider);
                    log.debug("Provincia {}: {} registros", provincia, datosProvider.size());
                }

            } catch (Exception e) {
                log.error("Error en provincia {}: {}", provincia, e.getMessage());
            }
        }

        log.info("Total recopilado: {} registros de {} provincias",
                todosLosDatos.size(), repositories.size());

        return todosLosDatos;
    }

    /**
     * Crea clave única para agrupación
     */
    private String crearClaveGrupo(Map<String, Object> registro, List<String> camposAgrupacion) {
        return camposAgrupacion.stream()
                .map(campo -> String.valueOf(registro.getOrDefault(campo, "")))
                .collect(Collectors.joining("|"));
    }

    /**
     * Aplica límites de paginación
     */
    private List<Map<String, Object>> aplicarLimites(
            List<Map<String, Object>> datos,
            ParametrosFiltrosDTO filtros) {

        int limite = filtros.getLimiteEfectivo();
        int offset = filtros.calcularOffset();

        // Aplicar offset
        if (offset > 0 && offset < datos.size()) {
            datos = datos.subList(offset, datos.size());
        }

        // Aplicar límite
        if (limite > 0 && limite < datos.size()) {
            datos = datos.subList(0, limite);
        }

        return datos;
    }

    // =============== MÉTODOS PÚBLICOS ===============

    public boolean validarConsolidacion(ParametrosFiltrosDTO filtros) {
        return filtros.esConsolidado() && !repositoryFactory.getAllRepositories().isEmpty();
    }

    public Object generarRespuestaConsolidadaOptima(List<Map<String, Object>> datos, String formato) {
        return datos != null && !datos.isEmpty() ? datos : Collections.emptyList();
    }
}