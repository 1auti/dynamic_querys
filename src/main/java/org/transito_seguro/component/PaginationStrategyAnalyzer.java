package org.transito_seguro.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoDatoKeyset;
import org.transito_seguro.model.CampoKeyset;
import org.transito_seguro.model.consolidacion.analisis.AnalisisPaginacion;

import java.lang.reflect.Array;
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
            log.info("Query consolidada detectada - Sin paginación keyset");
            return new AnalisisPaginacion(
                    EstrategiaPaginacion.SIN_PAGINACION,
                    false,
                    Collections.emptyList(),
                    "Query con GROUP BY no requiere paginación keyset"
            );
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








}
