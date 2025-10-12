package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.formula.functions.T;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoDatoKeyset;
import org.transito_seguro.model.AnalisisPaginacion;
import org.transito_seguro.model.CampoKeyset;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.model.CampoKeyset.getPrioridadTipo;

@Component
@Slf4j
public class PaginationStrategyAnalyzer {

    private static final List<String> CAMPOS_KEYSET_CANDIDATOS = Arrays.asList(
            "serie_equipo","id_tipo_infra", "fecha_infraccion", "id_estado", "id_punto_control","packedfile"
    );

    /**
     * Determinamos la estrategia de paginacion optima para una query
     * */
    public AnalisisPaginacion determinarEstrategia(String sql) {
        log.debug("Analizando estrategia de paginación para query");

        // 1. Si es consolidada (GROUP BY), no paginar
        if (esQueryConsolidada(sql)) {
            List<CampoKeyset> camposGroupBy = extraerCamposGroupBy(sql);

            if (camposGroupBy.size() >= 2) {
                log.info("Query consolidada con keyset: {} campos GROUP BY", camposGroupBy.size());
                return new AnalisisPaginacion(
                        EstrategiaPaginacion.KEY_COMPUESTO,
                        false,
                        camposGroupBy,
                        "Keyset basado en campos GROUP BY"
                );
            } else {
                log.info("Query consolidada sin keyset viable");
                return new AnalisisPaginacion(
                        EstrategiaPaginacion.SIN_PAGINACION,
                        false,
                        Collections.emptyList(),
                        "GROUP BY insuficiente para keyset"
                );
            }
        }

        // 2. Verificar si tiene i.id (ID de infracciones)
        boolean tieneIdInfracciones = tieneIdInfracciones(sql);

        // 3. Detectar campos disponibles en el SELECT
        List<CampoKeyset> camposDisponibles = detectarCamposDisponibles(sql);

        // 4. Determinar estrategia según disponibilidad
        if (tieneIdInfracciones && camposDisponibles.size() >= 3) {
            // KEYSET CON ID: Necesitamos i.id + al menos 3 campos más
            log.info("Estrategia: KEYSET_CON_ID (ID + {} campos)", camposDisponibles.size());
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEYSET_CON_ID,
                    true,
                    selecionarLosCamposParaKeysetConId(camposDisponibles),
                    "ID de infracciones disponible + campos suficientes"
            );
        } else if (!tieneIdInfracciones && camposDisponibles.size() >= 3) {
            // KEYSET COMPUESTO: Sin ID pero con suficientes campos
            log.info("Estrategia: KEYSET_COMPUESTO ({} campos)", camposDisponibles.size());
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.KEY_COMPUESTO,
                    false,
                    selecionarLosCamposParaKeysetCompuesto(camposDisponibles),
                    "Sin ID pero campos suficientes para keyset compuesto"
            );
        } else if (camposDisponibles.size() >= 1) {
            // FALLBACK: Usar LIMIT con algún campo para ordenar
            log.info("Estrategia: FALLBACK_LIMIT_ONLY");
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.FALLBACK_LIMIT_ONLY,
                    tieneIdInfracciones,
                    camposDisponibles,
                    "Campos insuficientes para keyset completo"
            );
        } else {
            // OFFSET: Paginación tradicional
            log.warn("Estrategia: OFFSET (no se detectaron campos para keyset)");
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.OFFSET,
                    false,
                    Collections.emptyList(),
                    "Campos insuficientes, usando paginación offset"
            );
        }
    }


    private List<CampoKeyset> detectarCamposDisponibles(String sql){

        List<CampoKeyset> campos = new ArrayList<>();
        Set<TipoDatoKeyset> tipoDatoKeysets = new HashSet<>();

        //Extramos el campo select
        Pattern selectPattern = Pattern.compile(
                "SELECT\\s+(.*?)s+FROM",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher matcher = selectPattern.matcher(sql);

        if(!matcher.find()){
            return  campos;
        }

        String selectClause = matcher.group(1);
        int prioridad = 0 ;

        // Buscar campos candidatos
        for (String candidato : CAMPOS_KEYSET_CANDIDATOS) {
            Pattern campoPattern = Pattern.compile(
                    "\\b([a-zA-Z_]+\\." + candidato + ")\\b",
                    Pattern.CASE_INSENSITIVE
            );
            Matcher campoMatcher = campoPattern.matcher(selectClause);

            if (campoMatcher.find()) {
                String nombreCompleto = campoMatcher.group(1);
                TipoDatoKeyset tipo = TipoDatoKeyset.detectarTipoDato(candidato);

                // Evitar duplicados de tipo (no dos fechas, no dos estados)
                if (tipo == TipoDatoKeyset.DATE || tipo == TipoDatoKeyset.TIMESTAMP) {
                    if (tipoDatoKeysets.contains(TipoDatoKeyset.DATE) ||
                            tipoDatoKeysets.contains(TipoDatoKeyset.TIMESTAMP)) {
                        continue; // Ya tenemos un campo de fecha
                    }
                }

                String nombreParametro = "last" +
                        candidato.substring(0, 1).toUpperCase() +
                        candidato.substring(1).replaceAll("_", "");

                campos.add(new CampoKeyset(nombreCompleto, nombreParametro, tipo, prioridad++));
                tipoDatoKeysets.add(tipo);

                log.debug("Campo keyset detectado: {} ({})", nombreCompleto, tipo);
            }
        }

        return campos;
    }

    /**
     * Detecta si la query es consolidada (GROUP BY)
     */
    private boolean esQueryConsolidada(String sql) {
        // Limpiar comentarios básicos
        String sqlLimpio = sql.replaceAll("--.*$", "").replaceAll("/\\*.*?\\*/", "");
        String upper = sqlLimpio.toUpperCase();

        // GROUP BY más específico (evita comentarios)
        Pattern groupByPattern = Pattern.compile(
                "\\bGROUP\\s+BY\\s+[^;]+?(?=\\s+(HAVING|ORDER|LIMIT|$)|$)",
                Pattern.CASE_INSENSITIVE
        );

        boolean tieneGroupBy = groupByPattern.matcher(upper).find();
        boolean tieneAgg = Pattern.compile("\\b(COUNT|SUM|AVG|MIN|MAX)\\s*\\(").matcher(sqlLimpio).find();

        return tieneGroupBy && tieneAgg;
    }


    /**
     * Detecta si la query tiene i.id (ID de infracciones) en el SELECT
     */
    private boolean tieneIdInfracciones(String sql) {
        // Buscar específicamente i.id o infracciones.id
        Pattern pattern = Pattern.compile(
                "SELECT\\s+.*?\\b(i\\.id|infracciones\\.id)\\b.*?FROM",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        return pattern.matcher(sql).find();
    }


    /**
     * Selecionamos los campos para KEYSET_CAMPOS_ID
     * */
    private List<CampoKeyset> selecionarLosCamposParaKeysetConId(List<CampoKeyset> disponibles ){
        // Ordenamos por priorirdad y tomar los primeros 3
        disponibles.sort(Comparator.comparingInt(CampoKeyset::getPrioridad));
        return  disponibles.subList(0,Math.min(3,disponibles.size()));
    }

    /** Selecionar los campos para KEY_SET_COMPUESTO
     * */
    private List<CampoKeyset> selecionarLosCamposParaKeysetCompuesto(List<CampoKeyset> disponibles){
        //Priorizar TEXT -> DATE -> INTERGER -> BOOLEAN
        disponibles.sort((a,b)-> {
            int prioridadA = getPrioridadTipo(a.getTipoDato());
            int prioridadB = getPrioridadTipo(b.getTipoDato());
            if(prioridadA != prioridadB){
                return Integer.compare(prioridadA, prioridadB);
            }
            return Integer.compare(a.getPrioridad(),b.getPrioridad());
        });
        return disponibles.subList(0,Math.min(4,disponibles.size()));
    }



    private List<CampoKeyset> extraerCamposGroupBy(String sql) {
        List<CampoKeyset> campos = new ArrayList<>();

        // Primero intentar GROUP BY con nombres de columnas
        Pattern patternNombres = Pattern.compile(
                "GROUP\\s+BY\\s+([a-zA-Z_][a-zA-Z0-9_\\.]*(?:\\s*,\\s*[a-zA-Z_][a-zA-Z0-9_\\.]*)*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcherNombres = patternNombres.matcher(sql);
        if (matcherNombres.find()) {
            String groupByClause = matcherNombres.group(1).trim();
            String[] camposArray = groupByClause.split("\\s*,\\s*");

            for (int i = 0; i < Math.min(camposArray.length, 3); i++) {
                String campo = camposArray[i].trim();
                if (!campo.isEmpty()) {
                    TipoDatoKeyset tipo = inferirTipoDato(campo);
                    campos.add(new CampoKeyset(campo,campo, tipo, i));
                }
            }

            log.debug("Campos GROUP BY extraídos (nombres): {}", campos);
            return campos;
        }

        // Si es GROUP BY numérico (1,2,3...), extraer campos del SELECT
        Pattern patternNumerico = Pattern.compile(
                "GROUP\\s+BY\\s+(\\d+(?:\\s*,\\s*\\d+)*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcherNumerico = patternNumerico.matcher(sql);
        if (matcherNumerico.find()) {
            String groupByClause = matcherNumerico.group(1).trim();
            String[] numerosArray = groupByClause.split("\\s*,\\s*");

            // Extraer campos del SELECT
            List<String> camposSelect = extraerCamposDelSelect(sql);

            for (int i = 0; i < Math.min(numerosArray.length, 3); i++) {
                int posicion = Integer.parseInt(numerosArray[i].trim()) - 1; // 1-based to 0-based

                if (posicion >= 0 && posicion < camposSelect.size()) {
                    String campo = camposSelect.get(posicion);

                    // Extraer solo el alias (después de AS)
                    String alias = extraerAlias(campo);

                    if (!alias.isEmpty()) {
                        TipoDatoKeyset tipo = inferirTipoDato(alias);
                        campos.add(new CampoKeyset(alias,campo, tipo, i));
                    }
                }
            }

            log.debug("Campos GROUP BY extraídos (numérico): {}", campos);
            return campos;
        }

        return campos;
    }

    // NUEVO: Extraer campos del SELECT
    private List<String> extraerCamposDelSelect(String sql) {
        List<String> campos = new ArrayList<>();

        Pattern selectPattern = Pattern.compile(
                "SELECT\\s+(.*?)\\s+FROM",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = selectPattern.matcher(sql);
        if (!matcher.find()) {
            return campos;
        }

        String selectClause = matcher.group(1).trim();

        // Dividir por comas (simple, no maneja funciones complejas perfectamente)
        String[] parts = selectClause.split(",");

        for (String part : parts) {
            campos.add(part.trim());
        }

        return campos;
    }

    // Extraer alias de un campo
    private String extraerAlias(String campo) {
        // Buscar "AS alias"
        Pattern aliasPattern = Pattern.compile(
                "\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = aliasPattern.matcher(campo);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }

        // Si no tiene AS, tomar lo último después del punto
        if (campo.contains(".")) {
            String[] parts = campo.split("\\.");
            return parts[parts.length - 1].trim();
        }

        // Si no, devolver el campo completo (limpiando espacios)
        return campo.replaceAll("\\s+", "").trim();
    }

    private TipoDatoKeyset inferirTipoDato(String campo){
        String lower = campo.toLowerCase();

        if(lower.contains("fecha") || lower.contains("date")){
            return TipoDatoKeyset.DATE;
        } else if (lower.contains("anio") || lower.contains("year")) {
            return  TipoDatoKeyset.INTEGER;
        }

        return TipoDatoKeyset.TEXT;
    }

}
