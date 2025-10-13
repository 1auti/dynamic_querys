package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.TipoCampo;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.CampoAnalizado;
import org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.service.QueryRegistryService;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.enums.TipoCampo.determinarTipoCampo;
import static org.transito_seguro.model.consolidacion.analisis.AnalisisConsolidacion.crearAnalisisVacio;
import static org.transito_seguro.utils.RegexUtils.*;

/**
 * Componente encargado de analizar queries SQL para determinar la estrategia
 * óptima de consolidación de datos.
 *
 * El análisis considera:
 * - Presencia de GROUP BY (query agregada vs cruda)
 * - Cardinalidad de campos de agrupación
 * - Estimación de registros resultantes
 * - Tipo de consolidación recomendado
 *
 * @author Sistema Tránsito Seguro
 * @version 2.0
 */
@Slf4j
@Component
public class QueryAnalyzer {

    @Autowired
    @Lazy
    private QueryRegistryService queryRegistryService;

    // ========== UMBRALES DE DECISIÓN ==========

    /**
     * Umbral para queries AGREGADAS con GROUP BY.
     * Si la estimación supera este valor, la query requiere streaming
     * incluso estando agregada.
     */
    private static final int UMBRAL_AGREGADA_STREAMING = 100_000;

    /**
     * Umbral para queries AGREGADAS consideradas "seguras" en memoria.
     * Bajo este valor, se puede cargar todo el resultado en memoria.
     */
    private static final int UMBRAL_AGREGADA_MEMORIA = 50_000;

    /**
     * Umbral para queries CRUDAS (sin GROUP BY).
     * Si COUNT(*) supera este valor, se FUERZA agregación automática.
     */
    private static final int UMBRAL_CRUDO_FORZAR_AGREGACION = 100_000;

    /**
     * Umbral intermedio para queries CRUDAS.
     * Entre 10k y 100k registros: procesar crudo pero con streaming.
     */
    private static final int UMBRAL_CRUDO_STREAMING = 10_000;

    // ========== CARDINALIDADES CONOCIDAS ==========

    /**
     * Mapa con cardinalidades estimadas de campos comunes.
     * Se usa para estimar cuántos registros retornará una query agregada.
     *
     * Key: nombre del campo (normalizado)
     * Value: cantidad estimada de valores únicos
     */
    private static final Map<String, Integer> CARDINALIDADES_CONOCIDAS;

    // Dentro del bloque static de CARDINALIDADES_CONOCIDAS:

    static {
        Map<String, Integer> map = new HashMap<>();

        // === UBICACIÓN ===
        map.put("provincia", 24);
        map.put("municipio", 500);
        map.put("localidad", 2_000);
        map.put("lugar", 5_000);
        map.put("ciudad", 1_000);           // ← NUEVO
        map.put("departamento", 300);       // ← NUEVO
        map.put("region", 10);              // ← NUEVO

        // === TIEMPO ===
        map.put("anio", 5);
        map.put("mes", 12);
        map.put("dia", 31);
        map.put("trimestre", 4);
        map.put("semestre", 2);
        map.put("dia_semana", 7);
        map.put("fecha", 365);
        map.put("hora", 24);
        map.put("mes_anio", 60);            // ← NUEVO: 5 años * 12 meses
        map.put("fecha_constatacion", 365); // ← NUEVO: Específico de tu query

        // === CATEGORIZACIÓN ===
        map.put("tipo_infraccion", 50);
        map.put("tipo", 20);
        map.put("estado", 5);
        map.put("estado_infraccion", 10);   // ← NUEVO: Específico de tu query
        map.put("enviado_a", 3);
        map.put("categoria", 10);
        map.put("gravedad", 3);

        // === EQUIPOS Y DISPOSITIVOS ===
        map.put("serie_equipo", 100);       // Ya existe

        // === IDENTIFICADORES COMUNES ===
        map.put("id", 10_000);              // ← NUEVO
        map.put("codigo", 1_000);           // ← NUEVO

        // === COMBINACIONES ESPECÍFICAS DE TU DOMINIO ===
        map.put("provincia_mes", 288);      // 24 * 12
        map.put("provincia_anio", 120);     // 24 * 5
        map.put("provincia_tipo", 1_200);   // 24 * 50
        map.put("mes_tipo", 600);           // 12 * 50

        // ← NUEVAS COMBINACIONES PARA TU QUERY:
        map.put("municipio_localidad", 10_000);           // 500 * 20
        map.put("serie_equipo_tipo", 5_000);              // 100 * 50
        map.put("localidad_tipo_infraccion", 100_000);    // 2000 * 50

        CARDINALIDADES_CONOCIDAS = Collections.unmodifiableMap(map);
    }

    //==================== MÉTODO PRINCIPAL DE ANÁLISIS ====================//

    /**
     * Analiza una query SQL para determinar la estrategia óptima de consolidación.
     *
     * Proceso de análisis:
     * 1. Extrae y parsea la cláusula SELECT
     * 2. Clasifica campos por tipo (ubicación, tiempo, numérico, etc.)
     * 3. Detecta presencia de GROUP BY
     * 4. Estima cantidad de registros según cardinalidad
     * 5. Determina tipo de consolidación óptimo
     *
     * @param query Query SQL a analizar
     * @return Análisis completo con estrategia recomendada
     */
    public AnalisisConsolidacion analizarParaConsolidacion(String query) {
        log.debug("=== INICIANDO ANÁLISIS DE QUERY ===");
        log.debug("Query: {}", query);

        try {
            // PASO 1: Extraer cláusula SELECT
            String selectClause = extraerSelectClause(query);
            if (selectClause == null) {
                log.warn("No se pudo extraer SELECT de la query");
                return crearAnalisisVacio();
            }

            // PASO 2: Parsear campos individuales
            List<CampoAnalizado> campos = parsearCamposSelect(selectClause);
            log.debug("Campos parseados: {}", campos.size());

            // PASO 3: Clasificar campos por tipo y propósito
            AnalisisConsolidacion analisisBase = clasificarCamposParaConsolidacion(campos, query);

            // PASO 4: Verificar si es consolidable
            if (!analisisBase.isEsConsolidable()) {
                log.warn("Query NO consolidable - No cumple requisitos mínimos");
                return analisisBase;
            }

            // PASO 5: Determinar tipo de consolidación y estimar registros
            return completarAnalisisConsolidacion(analisisBase, query);

        } catch (Exception e) {
            log.error("Error crítico analizando query: {}", e.getMessage(), e);
            return crearAnalisisVacio();
        }
    }

    //==================== COMPLETAR ANÁLISIS ====================//

    /**
     * Completa el análisis determinando el tipo de consolidación según
     * la presencia de GROUP BY y la estimación de registros.
     *
     * Lógica de decisión:
     * - CON GROUP BY → Query agregada (estimar por cardinalidad)
     * - SIN GROUP BY → Query cruda (requiere COUNT(*) dinámico)
     *
     * @param analisisBase Análisis básico con campos clasificados
     * @param query Query SQL original
     * @return Análisis completo con tipo y estimación
     */
    private AnalisisConsolidacion completarAnalisisConsolidacion(
            AnalisisConsolidacion analisisBase,
            String query) {

        boolean tieneGroupBy = tieneGroupBy(query);
        log.debug("Tiene GROUP BY: {}", tieneGroupBy);

        if (tieneGroupBy) {
            // Query con GROUP BY = datos ya agregados
            // → Estimar registros por cardinalidad de campos GROUP BY
            return analizarQueryAgregada(analisisBase, query);
        } else {
            // Query sin GROUP BY = datos crudos registro por registro
            // → No se puede estimar sin COUNT(*), requiere verificación dinámica
            return analizarQueryCruda(analisisBase, query);
        }
    }

    //==================== ANÁLISIS QUERY AGREGADA ====================//

    /**
     * Analiza una query que YA ESTÁ AGREGADA (con GROUP BY).
     *
     * Estimación de registros:
     * - Multiplica las cardinalidades de los campos en GROUP BY
     * - Ejemplo: GROUP BY provincia, mes → 24 * 12 = 288 registros
     *
     * Estrategia según estimación:
     * - < 50k registros: AGREGACION (cargar en memoria)
     * - 50k - 100k: AGREGACION con streaming
     * - > 100k: CRUDO con streaming (demasiados grupos para agregar más)
     *
     * @param analisisBase Análisis básico de campos
     * @param query Query SQL completa
     * @return Análisis con tipo AGREGACION o CRUDO según volumen
     */
    /**
     * Analiza una query que YA ESTÁ AGREGADA (con GROUP BY).
     *
     * Estimación de registros:
     * - Multiplica las cardinalidades de los campos en GROUP BY
     * - Ejemplo: GROUP BY provincia, mes → 24 * 12 = 288 registros
     *
     * Estrategia según estimación:
     * - < 50k registros: AGREGACION (cargar en memoria)
     * - 50k - 100k: AGREGACION_STREAMING (streaming preventivo)
     * - > 100k: AGREGACION_ALTO_VOLUMEN (streaming optimizado + agregación incremental)
     *
     * @param analisisBase Análisis básico de campos
     * @param query Query SQL completa
     * @return Análisis con tipo de consolidación apropiado
     */
    private AnalisisConsolidacion analizarQueryAgregada(
            AnalisisConsolidacion analisisBase,
            String query) {

        log.debug("--- Analizando QUERY AGREGADA (con GROUP BY) ---");

        // Extraer campos del GROUP BY
        List<String> camposGroupBy = extraerCamposGroupBy(query);
        log.debug("Campos GROUP BY detectados: {}", camposGroupBy);

        // Estimar registros basándose en cardinalidad de campos
        EstimacionRegistros estimacion = estimarRegistrosAgregados(camposGroupBy, analisisBase);

        // ========== DECISIÓN DE ESTRATEGIA ==========
        TipoConsolidacion tipo;
        String recomendacion;
        String estrategiaTecnica;

        if (estimacion.registrosEstimados < UMBRAL_AGREGADA_MEMORIA) {
            //  CASO 1: Bajo volumen → Procesamiento seguro en memoria
            tipo = TipoConsolidacion.AGREGACION;
            recomendacion = "Volumen bajo - Carga completa en memoria sin riesgos";
            estrategiaTecnica = "Consolidar grupos en HashMap, sumar valores acumulados";

            log.info("✓ Estrategia: AGREGACION en memoria ({} grupos estimados)",
                    estimacion.registrosEstimados);

        } else if (estimacion.registrosEstimados < UMBRAL_AGREGADA_STREAMING) {
            // ⚠ CASO 2: Volumen medio → Streaming preventivo
            tipo = TipoConsolidacion.AGREGACION_STREMING;
            recomendacion = "Volumen medio - Streaming recomendado para estabilidad";
            estrategiaTecnica = "Procesar por chunks de 5k registros, consolidar incrementalmente";

            log.warn("⚠ Estrategia: AGREGACION_STREAMING ({} grupos estimados - sobre umbral de {})",
                    estimacion.registrosEstimados, UMBRAL_AGREGADA_MEMORIA);

        } else {
            //  CASO 3: Alto volumen → Streaming optimizado obligatorio
            tipo = TipoConsolidacion.AGREGACION_ALTO_VOLUMEN;
            recomendacion = "Volumen ALTO - Streaming optimizado con agregación incremental obligatoria";
            estrategiaTecnica = String.format(
                    "Procesar por chunks de 10k registros, usar estructuras eficientes (ConcurrentHashMap), " +
                            "posible pre-agregación en BD si supera %,d grupos",
                    UMBRAL_AGREGADA_STREAMING * 2
            );

            log.error(" Estrategia: AGREGACION_ALTO_VOLUMEN ({} grupos estimados - SUPERA umbral de {})",
                    estimacion.registrosEstimados, UMBRAL_AGREGADA_STREAMING);

            // Advertencia adicional si el volumen es extremadamente alto
            if (estimacion.registrosEstimados > 500_000) {
                log.error(" VOLUMEN CRÍTICO: {} grupos - Considerar optimización en BD primero",
                        estimacion.registrosEstimados);
            }
        }

        // Construir explicación detallada
        String explicacionCompleta = String.format(
                "%s | %s | Técnica: %s | Confianza estimación: %.0f%%",
                estimacion.explicacion,
                recomendacion,
                estrategiaTecnica,
                estimacion.confianza * 100
        );

        // Log unificado del resultado
        log.info("📊 RESULTADO ANÁLISIS AGREGADA:");
        log.info("   ├─ Tipo: {}", tipo);
        log.info("   ├─ Estimación: {:,} registros (confianza: {:.0f}%)",
                estimacion.registrosEstimados, estimacion.confianza * 100);
        log.info("   ├─ Campos GROUP BY: {}", camposGroupBy);
        log.info("   └─ Estrategia: {}", recomendacion);

        // Retornar análisis completo
        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,                               // esConsolidable
                tipo,                               // tipo determinado correctamente
                estimacion.registrosEstimados,      // estimación calculada
                estimacion.confianza,               // confianza de la estimación
                explicacionCompleta                 // explicación detallada
        );
    }

    //==================== ANÁLISIS QUERY CRUDA ====================//

    /**
     * Analiza una query SIN GROUP BY (datos crudos).
     *
     * Problema: Sin agregación no podemos estimar cuántos registros
     * retornará sin ejecutar un COUNT(*) previo.
     *
     * Estrategia:
     * 1. Marcar como requiere COUNT(*) dinámico
     * 2. El servicio debe ejecutar COUNT(*) antes de procesar
     * 3. Según el COUNT(*) decidir si:
     *    - < 10k: Procesar crudo en memoria
     *    - 10k-100k: Procesar crudo con streaming
     *    - > 100k: FORZAR agregación automática
     *
     * @param analisisBase Análisis básico de campos
     * @param query Query SQL completa
     * @return Análisis marcado como CRUDO con estimación NULL
     */
    private AnalisisConsolidacion analizarQueryCruda(
            AnalisisConsolidacion analisisBase,
            String query) {

        log.warn("--- Analizando QUERY CRUDA (sin GROUP BY) ---");
        log.warn("⚠ Requiere COUNT(*) dinámico para determinar estrategia");

        // Intentar detectar filtros WHERE que limiten el resultado
        String explicacion = construirExplicacionQueryCruda(query);

        return new AnalisisConsolidacion(
                analisisBase.getCamposAgrupacion(),
                analisisBase.getCamposNumericos(),
                analisisBase.getCamposTiempo(),
                analisisBase.getCamposUbicacion(),
                analisisBase.getTipoPorCampo(),
                true,                           // esConsolidable
                TipoConsolidacion.CRUDO,        // tipo CRUDO por defecto
                null,                           // NO se puede estimar sin COUNT(*)
                0.0,                            // Confianza cero
                explicacion
        );
    }

    /**
     * Construye una explicación detallada para queries crudas.
     * Analiza si hay filtros WHERE que puedan limitar el volumen.
     *
     * @param query Query SQL
     * @return Explicación del análisis
     */
    private String construirExplicacionQueryCruda(String query) {
        StringBuilder explicacion = new StringBuilder();
        explicacion.append("Query SIN GROUP BY detectada - Datos crudos registro por registro. ");

        // Detectar si hay WHERE
        if (query.toUpperCase().contains("WHERE")) {
            explicacion.append("Contiene filtros WHERE que pueden limitar volumen. ");
        } else {
            explicacion.append("⚠ SIN filtros WHERE - Puede retornar TODOS los registros. ");
        }

        // Detectar LIMIT
        Pattern limitPattern = Pattern.compile("LIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher limitMatcher = limitPattern.matcher(query);
        if (limitMatcher.find()) {
            String limit = limitMatcher.group(1);
            explicacion.append("Tiene LIMIT ").append(limit).append(" - Volumen controlado. ");
        } else {
            explicacion.append("SIN LIMIT - Volumen potencialmente ilimitado. ");
        }

        explicacion.append("ACCIÓN REQUERIDA: Ejecutar COUNT(*) antes de procesar para determinar estrategia óptima.");

        return explicacion.toString();
    }

    //==================== ESTIMACIÓN DE REGISTROS ====================//

    /**
     * Clase interna para encapsular el resultado de una estimación.
     */
    private static class EstimacionRegistros {
        int registrosEstimados;     // Cantidad estimada de registros
        double confianza;           // Confianza en la estimación (0.0 - 1.0)
        String explicacion;         // Detalle del cálculo realizado

        EstimacionRegistros(int registros, double confianza, String explicacion) {
            this.registrosEstimados = registros;
            this.confianza = confianza;
            this.explicacion = explicacion;
        }
    }

    /**
     * Estima cuántos registros retornará una query agregada multiplicando
     * las cardinalidades de los campos en GROUP BY.
     *
     * Lógica:
     * 1. Para cada campo en GROUP BY, buscar su cardinalidad conocida
     * 2. Si no existe, estimar según el tipo de campo
     * 3. Multiplicar todas las cardinalidades (producto cartesiano)
     * 4. Calcular confianza según cuántos campos son conocidos
     *
     * Ejemplo:
     * GROUP BY provincia, mes, tipo
     * → 24 * 12 * 50 = 14,400 registros estimados
     *
     * @param camposGroupBy Lista de campos en la cláusula GROUP BY
     * @param analisis Análisis de consolidación con tipos de campos
     * @return Estimación de registros con nivel de confianza
     */
    private EstimacionRegistros estimarRegistrosAgregados(
            List<String> camposGroupBy,
            AnalisisConsolidacion analisis) {

        // Caso especial: Sin GROUP BY = 1 solo registro agregado
        if (camposGroupBy.isEmpty()) {
            return new EstimacionRegistros(
                    1,
                    1.0,
                    "Sin GROUP BY → 1 registro agregado (SELECT COUNT(*), SUM(...) FROM ...)"
            );
        }

        // Calcular producto de cardinalidades
        long producto = 1;
        int camposConocidos = 0;
        int camposDesconocidos = 0;
        List<String> detalleCalculo = new ArrayList<>();

        for (String campo : camposGroupBy) {
            Integer cardinalidad = CARDINALIDADES_CONOCIDAS.get(campo);

            if (cardinalidad != null) {
                // Campo con cardinalidad conocida
                producto *= cardinalidad;
                camposConocidos++;
                detalleCalculo.add(campo + "=" + cardinalidad);
                log.trace("Campo conocido: {} → {} valores únicos", campo, cardinalidad);

            } else {
                // Campo desconocido: estimar conservadoramente según tipo
                int cardinalidadAsumida = estimarCardinalidadDesconocida(campo, analisis);
                producto *= cardinalidadAsumida;
                camposDesconocidos++;
                detalleCalculo.add(campo + "≈" + cardinalidadAsumida);
                log.debug("Campo desconocido: {} → estimado ≈{} valores", campo, cardinalidadAsumida);
            }
        }

        // Calcular confianza según proporción de campos conocidos
        // 100% conocidos = confianza 1.0
        // 50% conocidos = confianza 0.5
        // 0% conocidos = confianza 0.0
        double confianza = camposDesconocidos == 0 ? 1.0 :
                (double) camposConocidos / camposGroupBy.size();

        // Limitar estimación a un máximo razonable (10M registros)
        int estimacionFinal = (int) Math.min(producto, 10_000_000);

        // Construir explicación legible
        String formula = String.join(" × ", detalleCalculo);
        String explicacion = String.format(
                "GROUP BY [%s]: %s = %,d registros estimados (%d campos conocidos, %d estimados)",
                String.join(", ", camposGroupBy),
                formula,
                estimacionFinal,
                camposConocidos,
                camposDesconocidos
        );

        log.debug("Estimación completada: {} registros (confianza: {:.0f}%)",
                estimacionFinal, confianza * 100);

        return new EstimacionRegistros(estimacionFinal, confianza, explicacion);
    }

    /**
     * Estima la cardinalidad de un campo desconocido basándose en su tipo.
     *
     * Heurísticas conservadoras:
     * - UBICACION: 500 (puede ser cualquier nivel de localización)
     * - TIEMPO: 365 (puede ser fecha completa)
     * - CATEGORIZACION: 20 (tipos, estados, categorías)
     * - IDENTIFICADOR: 1000 (códigos, series, IDs)
     * - NUMERICO: 100 (rangos posibles)
     * - DEFAULT: 100 (estimación conservadora)
     *
     * @param campo Nombre del campo
     * @param analisis Análisis de consolidación con tipos
     * @return Cardinalidad estimada
     */
    private int estimarCardinalidadDesconocida(String campo, AnalisisConsolidacion analisis) {
        TipoCampo tipo = analisis.getTipoPorCampo().get(campo);

        if (tipo == null) {
            log.warn("Campo sin tipo definido: {} - Usando cardinalidad por defecto", campo);
            return 100;
        }

        // Aplicar heurísticas según tipo
        switch (tipo) {
            case UBICACION:
                return 500;     // Rango medio entre municipio y localidad

            case TIEMPO:
                return 365;     // Asumir fecha completa (peor caso)

            case CATEGORIZACION:
                return 20;      // Cantidad típica de categorías

            case IDENTIFICADOR:
                return 1_000;   // IDs pueden ser muchos

            case NUMERICO_SUMA:
            case NUMERICO_COUNT:
                return 100;     // Rangos numéricos discretizados

            default:
                log.debug("Tipo {} sin heurística específica - Usando 100", tipo);
                return 100;
        }
    }

    //==================== EXTRACCIÓN DE CLÁUSULAS ====================//

    /**
     * Extrae los campos de la cláusula GROUP BY, resolviendo referencias posicionales.
     *
     * Maneja dos formatos:
     * - Nombres explícitos: "GROUP BY provincia, mes"
     * - Referencias numéricas: "GROUP BY 1, 2, 3" (resuelve según el orden del SELECT)
     *
     * Ejemplos:
     * - "GROUP BY provincia, mes" → ["provincia", "mes"]
     * - "GROUP BY x.provincia, DATE(fecha)" → ["provincia", "fecha"]
     * - "GROUP BY 1, 2" con SELECT provincia, mes → ["provincia", "mes"]
     *
     * @param query Query SQL completa
     * @return Lista de campos en GROUP BY (normalizados y resueltos)
     */
    private List<String> extraerCamposGroupBy(String query) {
        List<String> campos = new ArrayList<>();

        // Pattern para detectar GROUP BY hasta la siguiente cláusula
        Pattern groupByPattern = Pattern.compile(
                "GROUP\\s+BY\\s+([^HAVING|ORDER|LIMIT|;]+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = groupByPattern.matcher(query);
        if (!matcher.find()) {
            log.debug("No se encontró cláusula GROUP BY");
            return campos;
        }

        String groupByClause = matcher.group(1).trim();
        log.trace("GROUP BY extraído: {}", groupByClause);

        // Dividir por comas
        String[] camposRaw = groupByClause.split(",");

        // Verificar si usa referencias posicionales (números)
        boolean usaReferenciasNumericas = false;
        for (String campo : camposRaw) {
            if (campo.trim().matches("^\\d+$")) {
                usaReferenciasNumericas = true;
                break;
            }
        }

        if (usaReferenciasNumericas) {
            // ========== CASO: Referencias numéricas (GROUP BY 1, 2, 3) ==========
            log.debug("GROUP BY usa referencias posicionales - Extrayendo campos del SELECT");

            List<String> camposSelect = extraerNombresCamposSelect(query);

            for (String campoRaw : camposRaw) {
                String posicionStr = campoRaw.trim();

                if (posicionStr.matches("^\\d+$")) {
                    // Es una referencia numérica
                    int posicion = Integer.parseInt(posicionStr);

                    // Las posiciones en SQL son 1-indexed
                    if (posicion >= 1 && posicion <= camposSelect.size()) {
                        String nombreCampo = camposSelect.get(posicion - 1);
                        campos.add(nombreCampo);
                        log.trace("Posición {} → campo '{}'", posicion, nombreCampo);
                    } else {
                        log.warn("Referencia posicional {} fuera de rango (SELECT tiene {} campos)",
                                posicion, camposSelect.size());
                        // Agregar placeholder para no romper la estimación
                        campos.add("campo_" + posicion);
                    }
                } else {
                    // Mezcla de referencias numéricas y nombres (raro pero posible)
                    String campoNormalizado = normalizarNombreCampo(campoRaw);
                    if (!campoNormalizado.isEmpty()) {
                        campos.add(campoNormalizado);
                    }
                }
            }

        } else {
            // ========== CASO: Nombres explícitos (GROUP BY provincia, mes) ==========
            for (String campoRaw : camposRaw) {
                String campoNormalizado = normalizarNombreCampo(campoRaw);
                if (!campoNormalizado.isEmpty()) {
                    campos.add(campoNormalizado);
                }
            }
        }

        log.debug("Campos GROUP BY resueltos: {}", campos);
        return campos;
    }

    /**
     * Extrae los nombres finales de los campos del SELECT (después de AS o inferidos).
     *
     * Ejemplos:
     * - "provincia" → "provincia"
     * - "SUM(monto) AS total" → "total"
     * - "TO_CHAR(fecha, 'MM/YYYY') AS mes_anio" → "mes_anio"
     * - "i.cantidad" → "cantidad"
     *
     * @param query Query SQL completa
     * @return Lista ordenada de nombres de campos del SELECT
     */
    private List<String> extraerNombresCamposSelect(String query) {
        List<String> nombres = new ArrayList<>();

        try {
            // Extraer cláusula SELECT
            String selectClause = extraerSelectClause(query);
            if (selectClause == null) {
                log.warn("No se pudo extraer SELECT para resolver referencias numéricas");
                return nombres;
            }

            // Parsear campos
            List<String> camposRaw = dividirCamposInteligente(selectClause);

            for (String campoRaw : camposRaw) {
                String nombreFinal = extraerNombreFinalCampo(campoRaw.trim());
                nombres.add(nombreFinal);
                log.trace("Campo SELECT: '{}' → nombre final: '{}'", campoRaw, nombreFinal);
            }

        } catch (Exception e) {
            log.error("Error extrayendo nombres de campos SELECT: {}", e.getMessage(), e);
        }

        return nombres;
    }

    /**
     * Extrae el nombre final de un campo del SELECT.
     * Prioriza alias (AS), luego infiere del nombre del campo.
     *
     * @param expresion Expresión del campo (puede incluir funciones y alias)
     * @return Nombre final normalizado
     */
    private String extraerNombreFinalCampo(String expresion) {
        // Buscar alias con AS
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        if (aliasMatcher.matches()) {
            // Tiene alias explícito
            String alias = aliasMatcher.group(2).trim().toLowerCase();
            return alias.replaceAll("[\"'`]", ""); // Limpiar comillas
        }

        // Sin alias: inferir del campo base
        return normalizarNombreCampo(expresion);
    }

    /**
     * Normaliza el nombre de un campo removiendo prefijos y funciones.
     *
     * Ejemplos:
     * - "i.provincia" → "provincia"
     * - "DATE(fecha)" → "fecha"
     * - "TO_CHAR(i.fecha, 'MM/YYYY')" → "fecha"
     * - "COUNT(id)" → "count"
     *
     * @param campo Campo SQL (puede ser simple o con funciones)
     * @return Nombre normalizado
     */
    private String normalizarNombreCampo(String campo) {
        String limpio = campo.trim().toLowerCase();

        // Detectar funciones de fecha y extraer el campo interno
        // TO_CHAR(fecha, ...) → fecha
        Matcher toCharMatcher = Pattern.compile(
                "to_char\\s*\\(\\s*([a-zA-Z._]+).*\\)",
                Pattern.CASE_INSENSITIVE
        ).matcher(limpio);
        if (toCharMatcher.find()) {
            limpio = toCharMatcher.group(1);
        }

        // EXTRACT(YEAR FROM fecha) → anio
        if (limpio.matches(".*extract\\s*\\(\\s*year\\s+from.*")) {
            return "anio";
        }
        if (limpio.matches(".*extract\\s*\\(\\s*month\\s+from.*")) {
            return "mes";
        }
        if (limpio.matches(".*extract\\s*\\(\\s*day\\s+from.*")) {
            return "dia";
        }

        // DATE_TRUNC('month', fecha) → mes
        Matcher dateTruncMatcher = Pattern.compile(
                "date_trunc\\s*\\(\\s*'([^']+)'.*\\)",
                Pattern.CASE_INSENSITIVE
        ).matcher(limpio);
        if (dateTruncMatcher.find()) {
            String granularidad = dateTruncMatcher.group(1);
            if (granularidad.equals("month")) return "mes";
            if (granularidad.equals("year")) return "anio";
            if (granularidad.equals("day")) return "fecha";
        }

        // DATE(fecha) → fecha
        Matcher dateMatcher = Pattern.compile(
                "date\\s*\\(\\s*([a-zA-Z._]+)\\s*\\)",
                Pattern.CASE_INSENSITIVE
        ).matcher(limpio);
        if (dateMatcher.find()) {
            limpio = dateMatcher.group(1);
        }

        // Otras funciones: extraer el primer argumento
        // CONCAT(a, b) → a
        // COALESCE(campo, 'default') → campo
        Matcher funcionMatcher = Pattern.compile(
                "[a-z_]+\\s*\\(\\s*([a-zA-Z._]+).*\\)",
                Pattern.CASE_INSENSITIVE
        ).matcher(limpio);
        if (funcionMatcher.find()) {
            limpio = funcionMatcher.group(1);
        }

        // Remover prefijo de tabla: i.provincia → provincia
        limpio = limpio.replaceAll("^[a-z_]+\\.", "");

        // Remover comillas
        limpio = limpio.replaceAll("[\"'`]", "");

        return limpio;
    }

    /**
     * Extrae la cláusula SELECT completa de una query.
     *
     * @param query Query SQL
     * @return Cláusula SELECT sin el prefijo "SELECT"
     */
    private String extraerSelectClause(String query) {
        Matcher matcher = SELECT_PATTERN.matcher(query.trim());
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    //==================== PARSEO DE CAMPOS ====================//

    /**
     * Parsea la cláusula SELECT dividiéndola en campos individuales.
     * Respeta paréntesis de funciones y comillas en literales.
     *
     * @param selectClause Cláusula SELECT (sin el prefijo SELECT)
     * @return Lista de campos analizados
     */
    private List<CampoAnalizado> parsearCamposSelect(String selectClause) {
        List<CampoAnalizado> campos = new ArrayList<>();

        // Dividir por comas respetando paréntesis y comillas
        List<String> camposRaw = dividirCamposInteligente(selectClause);

        for (String campoRaw : camposRaw) {
            CampoAnalizado campo = analizarCampoIndividual(campoRaw.trim());
            if (campo != null) {
                campos.add(campo);
                log.trace("Campo parseado: {} → {} (tipo: {})",
                        campo.expresionOriginal, campo.nombreFinal, campo.tipo);
            }
        }

        log.debug("Parseados {} campos del SELECT", campos.size());
        return campos;
    }

    /**
     * Analiza un campo individual del SELECT, detectando:
     * - Alias (AS)
     * - Funciones de agregación
     * - Expresiones calculadas
     * - Tipo de campo
     *
     * @param expresion Expresión del campo (ej: "SUM(monto) AS total")
     * @return Campo analizado con metadatos
     */
    private CampoAnalizado analizarCampoIndividual(String expresion) {
        // Detectar alias con AS
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        String expresionLimpia;
        String nombreFinal;

        if (aliasMatcher.matches()) {
            // Tiene alias: "expresion AS alias"
            expresionLimpia = aliasMatcher.group(1).trim();
            nombreFinal = aliasMatcher.group(2).trim().toLowerCase();
            nombreFinal = nombreFinal.replaceAll("[\"'`]", ""); // Limpiar comillas
        } else {
            // Sin alias: usar nombre del campo
            expresionLimpia = expresion.trim();
            nombreFinal = expresionLimpia
                    .replaceAll("^[a-zA-Z_]+\\.", "")  // Remover prefijo tabla
                    .toLowerCase();
        }

        // Determinar características del campo
        TipoCampo tipo = determinarTipoCampo(expresionLimpia, nombreFinal);
        if(tipo == null){
         tipo = inferirTipoPorNombre(nombreFinal);
        }

        boolean esAgregacion = esExpresionAgregacion(expresionLimpia);
        boolean esCalculado = esExpresionCalculada(expresionLimpia);

        return new CampoAnalizado(
                expresion,
                expresionLimpia,
                nombreFinal,
                tipo,
                esAgregacion,
                esCalculado
        );
    }

    // Método auxiliar nuevo
    /**
     * Infiere el tipo de un campo basándose en su nombre cuando no se puede
     * determinar por su expresión SQL.
     *
     * Usa patrones comunes en nomenclatura de bases de datos para clasificar campos.
     *
     * @param nombre Nombre del campo (normalizado, sin prefijos)
     * @return Tipo de campo inferido, nunca null
     */
    private TipoCampo inferirTipoPorNombre(String nombre) {
        String nombreLower = nombre.toLowerCase();

        // ========== IDENTIFICADORES ==========
        if (nombreLower.matches(".*_(id|codigo|serie|clave|key)$") ||
                nombreLower.startsWith("id_") ||
                nombreLower.equals("id")) {
            return TipoCampo.IDENTIFICADOR;
        }

        // ========== UBICACIÓN ==========
        if (nombreLower.contains("provincia") ||
                nombreLower.contains("municipio") ||
                nombreLower.contains("localidad") ||
                nombreLower.contains("ciudad") ||
                nombreLower.contains("departamento") ||
                nombreLower.contains("region") ||
                nombreLower.contains("lugar") ||
                nombreLower.contains("ubicacion") ||
                nombreLower.matches(".*(pcia|prov|mun|loc).*")) {
            return TipoCampo.UBICACION;
        }

        // ========== TIEMPO ==========
        if (nombreLower.contains("fecha") ||
                nombreLower.contains("anio") ||
                nombreLower.contains("año") ||
                nombreLower.contains("mes") ||
                nombreLower.contains("dia") ||
                nombreLower.contains("hora") ||
                nombreLower.contains("trimestre") ||
                nombreLower.contains("semestre") ||
                nombreLower.contains("periodo") ||
                nombreLower.contains("timestamp") ||
                nombreLower.matches(".*(year|month|day|date|time).*")) {
            return TipoCampo.TIEMPO;
        }

        // ========== CATEGORIZACIÓN ==========
        if (nombreLower.contains("tipo") ||
                nombreLower.contains("estado") ||
                nombreLower.contains("categoria") ||
                nombreLower.contains("clasificacion") ||
                nombreLower.contains("gravedad") ||
                nombreLower.contains("nivel") ||
                nombreLower.contains("clase") ||
                nombreLower.contains("grupo") ||
                nombreLower.matches(".*_(cat|tipo|est|status).*")) {
            return TipoCampo.CATEGORIZACION;
        }

        // ========== NUMÉRICOS ==========
        if (nombreLower.matches("(total|suma|count|cantidad|monto|importe|valor|precio).*") ||
                nombreLower.matches(".*(total|suma|cantidad|monto|count|sum|avg|promedio)$") ||
                nombreLower.startsWith("num_") ||
                nombreLower.startsWith("cant_")) {
            return TipoCampo.NUMERICO_SUMA;
        }

        // ========== EQUIPOS Y DISPOSITIVOS ==========
        if (nombreLower.contains("serie_equipo") ||
                nombreLower.contains("equipo") ||
                nombreLower.contains("dispositivo") ||
                nombreLower.contains("sensor") ||
                nombreLower.contains("camara")) {
            return TipoCampo.IDENTIFICADOR; // Tratarlos como IDs
        }

        // ========== DESCRIPCIONES Y DETALLES ==========
        if (nombreLower.contains("descripcion") ||
                nombreLower.contains("detalle") ||
                nombreLower.contains("observacion") ||
                nombreLower.contains("comentario") ||
                nombreLower.contains("nota") ||
                nombreLower.matches(".*_(desc|det|obs).*")) {
            return TipoCampo.DETALLE;
        }

        // ========== DEFAULT: Analizar contexto adicional ==========
        // Si termina en vocal + consonante repetida, puede ser campo de texto
        if (nombreLower.matches(".*[aeiou][bcdfghjklmnpqrstvwxyz]{2,}$")) {
            return TipoCampo.DETALLE;
        }

        // Si es muy corto (1-2 caracteres), probablemente sea un código
        if (nombreLower.length() <= 2) {
            return TipoCampo.IDENTIFICADOR;
        }

        // Default conservador
        log.debug("Campo '{}' no coincide con patrones conocidos - Clasificando como DETALLE", nombre);
        return TipoCampo.DETALLE;
    }

    //==================== CLASIFICACIÓN DE CAMPOS ====================//

    /**
     * Clasifica los campos parseados según su rol en la consolidación:
     * - Campos de agrupación (ubicación, tiempo, categorización)
     * - Campos numéricos (para sumar/contar)
     * - Campos calculados
     *
     * @param campos Lista de campos parseados
     * @param query Query SQL completa
     * @return Análisis base con campos clasificados
     */
    private AnalisisConsolidacion clasificarCamposParaConsolidacion(
            List<CampoAnalizado> campos,
            String query) {

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
                    // Campos calculados pueden ser agrupación según contexto
                    if (campo.nombreFinal.contains("enviado") ||
                            campo.nombreFinal.contains("hacia") ||
                            campo.nombreFinal.contains("categoria")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                case IDENTIFICADOR:
                case DETALLE:
                    // Pueden ser agrupación en casos específicos
                    if (campo.nombreFinal.equals("serie_equipo") ||
                            campo.nombreFinal.equals("lugar")) {
                        camposAgrupacion.add(campo.nombreFinal);
                    }
                    break;

                default:
                    log.warn("Tipo de campo no manejado: {} ({})", campo.nombreFinal, campo.tipo);
                    break;
            }
        }

        // Una query es consolidable si tiene:
        // - Al menos 1 campo numérico para agregar
        // - Al menos 1 campo de agrupación
        boolean esConsolidable = !camposNumericos.isEmpty() && !camposAgrupacion.isEmpty();

        // Si no hay ubicación explícita, agregar "provincia" por defecto
        if (camposUbicacion.isEmpty() && esConsolidable) {
            camposUbicacion.add("provincia");
            if (!camposAgrupacion.contains("provincia")) {
                camposAgrupacion.add("provincia");
            }
        }

        log.info("Clasificación completada - Consolidable: {}, Agrupación: {}, Numéricos: {}, Ubicación: {}, Tiempo: {}",
                esConsolidable, camposAgrupacion.size(), camposNumericos.size(),
                camposUbicacion.size(), camposTiempo.size());

        // Retornar análisis base (sin tipo ni estimación aún)
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

    //==================== MÉTODOS UTILITARIOS ====================//

    /**
     * Divide una cláusula SELECT en campos individuales respetando:
     * - Paréntesis anidados (funciones)
     * - Comillas (literales)
     *
     * Ejemplo: "provincia, SUM(monto), CONCAT(a, ',', b) AS full"
     * → ["provincia", "SUM(monto)", "CONCAT(a, ',', b) AS full"]
     *
     * @param selectClause Cláusula SELECT
     * @return Lista de campos sin dividir incorrectamente
     */
    private List<String> dividirCamposInteligente(String selectClause) {
        List<String> campos = new ArrayList<>();
        StringBuilder campoActual = new StringBuilder();
        int nivelParentesis = 0;
        boolean enComillas = false;
        char tipoComilla = 0;

        for (char c : selectClause.toCharArray()) {
            if (!enComillas && (c == '\'' || c == '"')) {
                // Entrar en modo comillas
                enComillas = true;
                tipoComilla = c;
            } else if (enComillas && c == tipoComilla) {
                // Salir de modo comillas
                enComillas = false;
            } else if (!enComillas) {
                // Fuera de comillas: contar paréntesis y dividir por comas
                if (c == '(') {
                    nivelParentesis++;
                } else if (c == ')') {
                    nivelParentesis--;
                } else if (c == ',' && nivelParentesis == 0) {
                    // Coma separadora (fuera de funciones)
                    campos.add(campoActual.toString().trim());
                    campoActual = new StringBuilder();
                    continue;
                }
            }
            campoActual.append(c);
        }

        // Agregar último campo
        if (campoActual.length() > 0) {
            campos.add(campoActual.toString().trim());
        }

        return campos;
    }

    /**
     * Detecta si una query contiene GROUP BY.
     *
     * @param sql Query SQL
     * @return true si tiene GROUP BY
     */
    public static boolean tieneGroupBy(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        // Normalizar espacios y convertir a minúsculas
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
     * Determina si una query es consolidable (tiene agregación o GROUP BY).
     *
     * @param sql Query SQL
     * @return true si es consolidable
     */
    public static boolean esQueryConsolidable(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        return tieneGroupBy(sql);
    }

    //==================== GETTERS PARA UMBRALES (testing/config) ====================//

    public static int getUmbralAgregadaStreaming() {
        return UMBRAL_AGREGADA_STREAMING;
    }

    public static int getUmbralAgregadaMemoria() {
        return UMBRAL_AGREGADA_MEMORIA;
    }

    public static int getUmbralCrudoForzarAgregacion() {
        return UMBRAL_CRUDO_FORZAR_AGREGACION;
    }

    public static int getUmbralCrudoStreaming() {
        return UMBRAL_CRUDO_STREAMING;
    }
}