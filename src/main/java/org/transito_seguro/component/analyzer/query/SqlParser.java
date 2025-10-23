package org.transito_seguro.component.analyzer.query;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.transito_seguro.utils.RegexUtils.CAMPO_AS_PATTERN;
import static org.transito_seguro.utils.RegexUtils.SELECT_PATTERN;


/**
 * Componente especializado en el parseo y extracción de cláusulas SQL.
 *
 * Responsabilidades:
 * - Extraer SELECT, WHERE, GROUP BY
 * - Dividir campos respetando paréntesis y comillas
 * - Normalizar nombres de campos
 * - Resolver referencias posicionales (GROUP BY 1, 2, 3)
 *
 */
@Slf4j
@Component
public class SqlParser {

    /**
     * Extrae la cláusula SELECT completa de una query.
     *
     * @param query Query SQL
     * @return Cláusula SELECT sin el prefijo "SELECT"
     */
    public String extraerSelectClause(String query) {
        Matcher matcher = SELECT_PATTERN.matcher(query.trim());
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Extrae la cláusula WHERE de una query SQL.
     *
     * @param query Query SQL
     * @return Contenido de la cláusula WHERE o null si no existe
     */
    public String extraerWhereClause(String query) {
        Pattern wherePattern = Pattern.compile(
                "WHERE\\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|;|$)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        Matcher matcher = wherePattern.matcher(query);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Extrae los campos de la cláusula GROUP BY, resolviendo referencias posicionales.
     *
     * Maneja dos formatos:
     * - Nombres explícitos: "GROUP BY provincia, mes"
     * - Referencias numéricas: "GROUP BY 1, 2, 3" (resuelve según el orden del SELECT)
     *
     * @param query Query SQL completa
     * @return Lista de campos en GROUP BY (normalizados y resueltos)
     */
    public List<String> extraerCamposGroupBy(String query) {
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

        String[] camposRaw = groupByClause.split(",");

        // Verificar si usa referencias posicionales (números)
        boolean usaReferenciasNumericas = Arrays.stream(camposRaw)
                .anyMatch(campo -> campo.trim().matches("^\\d+$"));

        if (usaReferenciasNumericas) {
            // Resolver posiciones numéricas
            List<String> camposSelect = extraerNombresCamposSelect(query);

            for (String campoRaw : camposRaw) {
                String posicionStr = campoRaw.trim();

                if (posicionStr.matches("^\\d+$")) {
                    int posicion = Integer.parseInt(posicionStr);

                    if (posicion >= 1 && posicion <= camposSelect.size()) {
                        String nombreCampo = camposSelect.get(posicion - 1);
                        campos.add(nombreCampo);
                        log.trace("Posición {} → campo '{}'", posicion, nombreCampo);
                    } else {
                        log.warn("Referencia posicional {} fuera de rango", posicion);
                        campos.add("campo_" + posicion);
                    }
                } else {
                    String campoNormalizado = normalizarNombreCampo(campoRaw);
                    if (!campoNormalizado.isEmpty()) {
                        campos.add(campoNormalizado);
                    }
                }
            }
        } else {
            // Nombres explícitos
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
     * Extrae los nombres finales de los campos del SELECT.
     *
     * @param query Query SQL completa
     * @return Lista ordenada de nombres de campos del SELECT
     */
    public List<String> extraerNombresCamposSelect(String query) {
        List<String> nombres = new ArrayList<>();

        try {
            String selectClause = extraerSelectClause(query);
            if (selectClause == null) {
                return nombres;
            }

            List<String> camposRaw = dividirCamposInteligente(selectClause);

            for (String campoRaw : camposRaw) {
                String nombreFinal = extraerNombreFinalCampo(campoRaw.trim());
                nombres.add(nombreFinal);
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
     * @param expresion Expresión del campo
     * @return Nombre final normalizado
     */
    public String extraerNombreFinalCampo(String expresion) {
        Matcher aliasMatcher = CAMPO_AS_PATTERN.matcher(expresion);

        if (aliasMatcher.matches()) {
            String alias = aliasMatcher.group(2).trim().toLowerCase();
            return alias.replaceAll("[\"'`]", "");
        }

        return normalizarNombreCampo(expresion);
    }

    /**
     * Normaliza el nombre de un campo removiendo prefijos y funciones.
     *
     * Ejemplos:
     * - "i.provincia" → "provincia"
     * - "DATE(fecha)" → "fecha"
     * - "TO_CHAR(i.fecha, 'MM/YYYY')" → "fecha"
     *
     * @param campo Campo SQL
     * @return Nombre normalizado
     */
    public String normalizarNombreCampo(String campo) {
        String limpio = campo.trim().toLowerCase();

        // TO_CHAR(fecha, ...) → fecha
        Matcher toCharMatcher = Pattern.compile(
                "to_char\\s*\\(\\s*([a-zA-Z._]+).*\\)",
                Pattern.CASE_INSENSITIVE
        ).matcher(limpio);
        if (toCharMatcher.find()) {
            limpio = toCharMatcher.group(1);
        }

        // EXTRACT(YEAR FROM fecha) → anio
        if (limpio.matches(".*extract\\s*\\(\\s*year\\s+from.*")) return "anio";
        if (limpio.matches(".*extract\\s*\\(\\s*month\\s+from.*")) return "mes";
        if (limpio.matches(".*extract\\s*\\(\\s*day\\s+from.*")) return "dia";

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

        // Otras funciones: extraer primer argumento
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
     * Divide una cláusula SELECT en campos individuales respetando:
     * - Paréntesis anidados (funciones)
     * - Comillas (literales)
     *
     * @param selectClause Cláusula SELECT
     * @return Lista de campos
     */
    public List<String> dividirCamposInteligente(String selectClause) {
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
                if (c == '(') {
                    nivelParentesis++;
                } else if (c == ')') {
                    nivelParentesis--;
                } else if (c == ',' && nivelParentesis == 0) {
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
     * Detecta si una query contiene GROUP BY.
     *
     * @param sql Query SQL
     * @return true si tiene GROUP BY
     */
    public static boolean tieneGroupBy(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        String sqlNormalizado = sql
                .replaceAll("\\s+", " ")
                .toLowerCase()
                .trim()
                .replaceAll("--[^\n]*", "")
                .replaceAll("/\\*.*?\\*/", "");

        return sqlNormalizado.matches(".*\\bgroup\\s+by\\b.*");
    }

    /**
     * Limpia comentarios de una query SQL.
     *
     * @param sql Query SQL
     * @return SQL sin comentarios
     */
    public String limpiarComentariosSQL(String sql) {
        return sql.replaceAll("--[^\n\r]*", "")
                .replaceAll("/\\*.*?\\*/", "")
                .replaceAll("\\s+", " ")
                .trim();
    }


}
