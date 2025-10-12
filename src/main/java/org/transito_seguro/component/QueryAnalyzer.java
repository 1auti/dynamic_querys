package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.CampoAnalizado;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.enums.TipoCampo.determinarTipoCampo;
import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;
import static org.transito_seguro.utils.RegexUtils.*;

@Slf4j
@Component
public class QueryAnalyzer {

    // ========== CARDINALIDADES CONOCIDAS ==========
    
    /**
     * Mapa con cardinalidades estimadas de campos comunes.
     * Key: nombre del campo, Value: cantidad estimada de valores únicos
     */
   private static final Map<String, Integer> CARDINALIDADES_CONOCIDAS;

static {
    Map<String, Integer> map = new HashMap<>();

    // Ubicación
    map.put("provincia", 24);
    map.put("municipio", 500);
    map.put("localidad", 2000);
    map.put("lugar", 5000);

    // Tiempo (año completo)
    map.put("anio", 5);
    map.put("mes", 12);
    map.put("dia", 31);
    map.put("trimestre", 4);
    map.put("semestre", 2);
    map.put("dia_semana", 7);
    map.put("fecha", 365); // ~1 año de datos

    // Categorización
    map.put("tipo_infraccion", 50);
    map.put("tipo", 20);
    map.put("estado", 5);
    map.put("enviado_a", 3); // [JUZGADO, ANSV, null]
    map.put("categoria", 10);

    // Equipos
    map.put("serie_equipo", 100);

    // Combinaciones comunes
    map.put("provincia_mes", 288);      // 24 * 12
    map.put("provincia_anio", 120);     // 24 * 5
    map.put("provincia_tipo", 1200);    // 24 * 50

    CARDINALIDADES_CONOCIDAS = Collections.unmodifiableMap(map);
}

    
    /**
     * Umbral para considerar una query como AGREGADA vs CRUDA.
     * Si la estimación es < este valor, es AGREGADA (segura para memoria).
     */
    private static final int UMBRAL_AGREGADA = 50000;

    //============================ ANALISIS DE CONSOLIDACION ================================================//

    /**
     * Analiza una query SQL para determinar estrategia de consolidación.
     * Ahora incluye estimación de registros y tipo de consolidación.
     * 
     * @param query Query SQL a analizar
     * @return Análisis completo con tipo y estimación
     */
    public AnalisisConsolidacion analizarParaConsolidacion(String query) {
        log.debug("Analizando query SQL para consolidación dinámica");

        try {
            // 1. Extraer SELECT clause
            String selectClause = extraerSelectClause(query);
            if (selectClause == null) {
                log.warn("No se pudo extraer SELECT de la query");
                return crearAnalisisVacio();
            }

            // 2. Extraer campos individuales del SELECT
            List<CampoAnalizado> campos = parsearCamposSelect(selectClause);

            // 3. Clasificar campos por tipo y propósito
            AnalisisConsolidacion analisisBase = clasificarCamposParaConsolidacion(campos, query);
            
            // 4. Si no es consolidable, retornar análisis vacío
            if (!analisisBase.isEsConsolidable()) {
                return analisisBase;
            }
            
            // 5. Determinar tipo de consolidación y estimar registros
            return completarAnalisisConsolidacion(analisisBase, query);

        } catch (Exception e) {
            log.error("Error analizando query para consolidación: {}", e.getMessage(), e);
            return crearAnalisisVacio();
        }
    }

    /**
     * Completa el análisis determinando tipo de consolidación y estimando registros.
     * 
     * @param analisisBase Análisis básico con campos clasificados
     * @param query Query SQL original
     * @return Análisis completo con estimaciones
     */
    private AnalisisConsolidacion completarAnalisisConsolidacion(
            AnalisisConsolidacion analisisBase, 
            String query) {
        
        boolean tieneGroupBy = tieneGroupBy(query);
        
        if (tieneGroupBy) {
            // Query con GROUP BY = AGREGADA (bien diseñada)
            return analizarQueryAgregada(analisisBase, query);
        } else {
            // Query sin GROUP BY = CRUDA (mal diseñada)
            return analizarQueryCruda(analisisBase);
        }
    }

    /**
     * Analiza una query AGREGADA (con GROUP BY).
     * Estima cuántos registros retornará basándose en la cardinalidad de campos de agrupación.
     * 
     * @param analisisBase Análisis básico
     * @param query Query SQL
     * @return Análisis completo con estimación
     */
    private AnalisisConsolidacion analizarQueryAgregada(
            AnalisisConsolidacion analisisBase, 
            String query) {
        
        // Extraer campos del GROUP BY
        List<String> camposGroupBy = extraerCamposGroupBy(query);
        
        // Estimar registros basándose en cardinalidad
        EstimacionRegistros estimacion = estimarRegistrosAgregados(camposGroupBy, analisisBase);
        
        // Determinar tipo según estimación
        TipoConsolidacion tipo = estimacion.registrosEstimados < UMBRAL_AGREGADA
            ? TipoConsolidacion.AGREGACION
            : TipoConsolidacion.CRUDO;
        
        log.info("Query AGREGADA detectada - Estimación: {} registros (confianza: {:.0f}%) - Tipo: {}",
                 estimacion.registrosEstimados, 
                 estimacion.confianza * 100,
                 tipo);
        
        return new AnalisisConsolidacion(
            analisisBase.getCamposAgrupacion(),
            analisisBase.getCamposNumericos(),
            analisisBase.getCamposTiempo(),
            analisisBase.getCamposUbicacion(),
            analisisBase.getTipoPorCampo(),
            true,
            tipo,
            estimacion.registrosEstimados,
            estimacion.confianza,
            estimacion.explicacion
        );
    }

    /**
     * Analiza una query CRUDA (sin GROUP BY).
     * No puede estimar registros, necesitará COUNT(*) dinámico.
     * 
     * @param analisisBase Análisis básico
     * @return Análisis completo marcado como CRUDA
     */
    private AnalisisConsolidacion analizarQueryCruda(AnalisisConsolidacion analisisBase) {
        
        log.warn("Query CRUDA detectada (sin GROUP BY) - Necesitará streaming y COUNT(*) dinámico");
        
        return new AnalisisConsolidacion(
            analisisBase.getCamposAgrupacion(),
            analisisBase.getCamposNumericos(),
            analisisBase.getCamposTiempo(),
            analisisBase.getCamposUbicacion(),
            analisisBase.getTipoPorCampo(),
            true,
            TipoConsolidacion.CRUDO,
            null,  // No se puede estimar sin ejecutar
            0.0,   // Confianza cero
            "Query sin GROUP BY - requiere COUNT(*) dinámico para estimar registros"
        );
    }

    /**
     * Extrae los campos del GROUP BY de una query.
     * 
     * @param query Query SQL
     * @return Lista de campos en el GROUP BY
     */
    private List<String> extraerCamposGroupBy(String query) {
        List<String> campos = new ArrayList<>();
        
        // Pattern para detectar GROUP BY
        Pattern groupByPattern = Pattern.compile(
            "GROUP\\s+BY\\s+([^HAVING|ORDER|LIMIT|;]+)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = groupByPattern.matcher(query);
        if (matcher.find()) {
            String groupByClause = matcher.group(1).trim();
            
            // Dividir por comas y limpiar
            String[] camposRaw = groupByClause.split(",");
            for (String campo : camposRaw) {
                String campoLimpio = campo.trim()
                    .replaceAll("^[a-zA-Z_]+\\.", "")  // Remover prefijo de tabla
                    .toLowerCase();
                campos.add(campoLimpio);
            }
        }
        
        log.debug("Campos GROUP BY detectados: {}", campos);
        return campos;
    }

    /**
     * Clase interna para resultado de estimación.
     */
    private static class EstimacionRegistros {
        int registrosEstimados;
        double confianza;
        String explicacion;
        
        EstimacionRegistros(int registros, double confianza, String explicacion) {
            this.registrosEstimados = registros;
            this.confianza = confianza;
            this.explicacion = explicacion;
        }
    }

    /**
     * Estima cuántos registros retornará una query agregada.
     * Usa cardinalidades conocidas y multiplica las cardinalidades de cada campo.
     * 
     * @param camposGroupBy Campos en el GROUP BY
     * @param analisis Análisis de consolidación
     * @return Estimación de registros
     */
    private EstimacionRegistros estimarRegistrosAgregados(
            List<String> camposGroupBy,
            AnalisisConsolidacion analisis) {
        
        if (camposGroupBy.isEmpty()) {
            return new EstimacionRegistros(1, 1.0, "Sin GROUP BY = 1 registro");
        }
        
        // Calcular producto de cardinalidades
        long producto = 1;
        int camposConocidos = 0;
        int camposDesconocidos = 0;
        List<String> detalleCalculo = new ArrayList<>();
        
        for (String campo : camposGroupBy) {
            Integer cardinalidad = CARDINALIDADES_CONOCIDAS.get(campo);
            
            if (cardinalidad != null) {
                producto *= cardinalidad;
                camposConocidos++;
                detalleCalculo.add(campo + ":" + cardinalidad);
            } else {
                // Campo desconocido: asumir cardinalidad conservadora
                int cardinalidadAsumida = estimarCardinalidadDesconocida(campo, analisis);
                producto *= cardinalidadAsumida;
                camposDesconocidos++;
                detalleCalculo.add(campo + ":~" + cardinalidadAsumida);
            }
        }
        
        // Calcular confianza
        double confianza = camposDesconocidos == 0 ? 1.0 :
                          (double) camposConocidos / camposGroupBy.size();
        
        // Limitar estimación a un máximo razonable
        int estimacionFinal = (int) Math.min(producto, 10_000_000);
        
        String explicacion = String.format(
            "GROUP BY %s: %s = %d registros estimados",
            camposGroupBy,
            String.join(" × ", detalleCalculo),
            estimacionFinal
        );
        
        return new EstimacionRegistros(estimacionFinal, confianza, explicacion);
    }

    /**
     * Estima la cardinalidad de un campo desconocido basándose en su tipo.
     * 
     * @param campo Nombre del campo
     * @param analisis Análisis de consolidación
     * @return Cardinalidad estimada
     */
    private int estimarCardinalidadDesconocida(String campo, AnalisisConsolidacion analisis) {
        TipoCampo tipo = analisis.getTipoPorCampo().get(campo);
        
        if (tipo == null) {
            return 100;  // Default conservador
        }
        
        switch (tipo) {
            case UBICACION:
                return 500;  // Puede ser municipio, localidad, etc.
            case TIEMPO:
                return 365;  // Puede ser fecha
            case CATEGORIZACION:
                return 20;   // Tipos, categorías, estados
            case IDENTIFICADOR:
                return 1000; // IDs, series, códigos
            case NUMERICO_SUMA:
            case NUMERICO_COUNT:
                return 100;  // Rangos numéricos
            default:
                return 100;
        }
    }

    /**
     * Extrae la cláusula SELECT completa
     */
    private String extraerSelectClause(String query) {
        Matcher matcher = SELECT_PATTERN.matcher(query.trim());
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Parsea campos individuales del SELECT
     */
    private List<CampoAnalizado> parsearCamposSelect(String selectClause) {
        List<CampoAnalizado> campos = new ArrayList<>();

        // Dividir por comas respetando paréntesis y funciones
        List<String> camposRaw = dividirCamposInteligente(selectClause);

        for (String campoRaw : camposRaw) {
            CampoAnalizado campo = analizarCampoIndividual(campoRaw.trim());
            if (campo != null) {
                campos.add(campo);
                log.trace("Campo parseado: {} -> {}", campo.expresionOriginal, campo.nombreFinal);
            }
        }

        log.debug("Parseados {} campos del SELECT", campos.size());
        return campos;
    }

    /**
     * Analiza un campo individual del SELECT
     */
    private CampoAnalizado analizarCampoIndividual(String expresion) {
        // Detectar si tiene alias (AS)
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        String expresionLimpia;
        String nombreFinal;

        if (aliasMatcher.matches()) {
            expresionLimpia = aliasMatcher.group(1).trim();
            nombreFinal = aliasMatcher.group(2).trim().toLowerCase();
            // Remover comillas del alias
            nombreFinal = nombreFinal.replaceAll("[\"']", "");
        } else {
            expresionLimpia = expresion.trim();
            // Extraer nombre del campo sin prefijo de tabla (x.campo -> campo)
            nombreFinal = expresionLimpia.replaceAll("^[a-zA-Z_]+\\.", "").toLowerCase();
        }

        // Determinar tipo de campo
        TipoCampo tipo = determinarTipoCampo(expresionLimpia, nombreFinal);
        boolean esAgregacion = esExpresionAgregacion(expresionLimpia);
        boolean esCalculado = esExpresionCalculada(expresionLimpia);

        return new CampoAnalizado(expresion, expresionLimpia, nombreFinal, tipo, esAgregacion, esCalculado);
    }

    /**
     * Clasifica campos para estrategia de consolidación
     */
    private AnalisisConsolidacion clasificarCamposParaConsolidacion(List<CampoAnalizado> campos, String query) {
        List<String> camposAgrupacion = new ArrayList<>();
        List<String> camposNumericos = new ArrayList<>();
        List<String> camposTiempo = new ArrayList<>();
        List<String> camposUbicacion = new ArrayList<>();
        Map<String, TipoCampo> tipoPorCampo = new HashMap<>();

        for (CampoAnalizado campo : campos) {
            tipoPorCampo.put(campo.nombreFinal, campo.tipo);

            switch (campo.tipo) {
                case UBICACION:
                    camposUbicacion.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case CATEGORIZACION:
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case TIEMPO:
                    camposTiempo.add(campo.nombreFinal);
                    camposAgrupacion.add(campo.nombreFinal);
                    break;

                case NUMERICO_SUMA:
                case NUMERICO_COUNT:
                    camposNumericos.add(campo.nombreFinal);
                    break;

                case CALCULADO:
                    // Los campos calculados pueden ser agrupación dependiendo del contexto
                    if (campo.nombreFinal.contains("enviado") || campo.nombreFinal.contains("hacia")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                case IDENTIFICADOR:
                case DETALLE:
                    // Generalmente no se usan para consolidación, pero pueden ser agrupación en algunos casos
                    if (campo.nombreFinal.equals("serie_equipo") || campo.nombreFinal.equals("lugar")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;
            }
        }

        // Determinar si es consolidable
        boolean esConsolidable = !camposNumericos.isEmpty() && !camposAgrupacion.isEmpty();

        // Si no hay campos de ubicación explícitos, agregar "provincia" como default
        if (camposUbicacion.isEmpty() && esConsolidable) {
            camposUbicacion.add("provincia");
            if (!camposAgrupacion.contains("provincia")) {
                camposAgrupacion.add("provincia");
            }
        }

        log.info("Análisis base completado - Consolidable: {}, Agrupación: {}, Numéricos: {}, Ubicación: {}",
                esConsolidable, camposAgrupacion.size(), camposNumericos.size(), camposUbicacion.size());

        // Retornar análisis base (sin tipo ni estimación todavía)
        return new AnalisisConsolidacion(
            camposAgrupacion,
            camposNumericos,
            camposTiempo,
            camposUbicacion,
            tipoPorCampo,
            esConsolidable,
            null,  // Tipo se determina después
            null,  // Estimación se calcula después
            0.0,   // Confianza se calcula después
            ""     // Explicación se genera después
        );
    }

    // =============== MÉTODOS UTILITARIOS ===============

    private List<String> dividirCamposInteligente(String selectClause) {
        List<String> campos = new ArrayList<>();
        StringBuilder campoActual = new StringBuilder();
        int nivelParentesis = 0;
        boolean enComillas = false;
        char tipoComilla = 0;

        for (char c : selectClause.toCharArray()) {
            if (!enComillas && (c == '\'' || c == '"')) {
                enComillas = true;
                tipoComilla = c;
            } else if (enComillas && c == tipoComilla) {
                enComillas = false;
            } else if (!enComillas) {
                if (c == '(') nivelParentesis++;
                else if (c == ')') nivelParentesis--;
                else if (c == ',' && nivelParentesis == 0) {
                    campos.add(campoActual.toString().trim());
                    campoActual = new StringBuilder();
                    continue;
                }
            }
            campoActual.append(c);
        }

        if (campoActual.length() > 0) {
            campos.add(campoActual.toString().trim());
        }

        return campos;
    }

    /**
     * Detecta si una query SQL contiene GROUP BY
     *
     * @param sql Query SQL a analizar
     * @return true si contiene GROUP BY
     */
    public static boolean tieneGroupBy(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        // Normalizar
        String sqlNormalizado = sql
                .replaceAll("\\s+", " ")
                .toLowerCase()
                .trim();

        // Remover comentarios
        sqlNormalizado = sqlNormalizado
                .replaceAll("--[^\n]*", "")
                .replaceAll("/\\*.*?\\*/", "");

        // Buscar GROUP BY como palabra completa
        return sqlNormalizado.matches(".*\\bgroup\\s+by\\b.*");
    }

    /**
     * Analiza una query y determina si es consolidable
     *
     * @param sql Query SQL
     * @return true si es consolidable (tiene GROUP BY o funciones agregadas)
     */
    public static boolean esQueryConsolidable(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        return tieneGroupBy(sql);
    }
}