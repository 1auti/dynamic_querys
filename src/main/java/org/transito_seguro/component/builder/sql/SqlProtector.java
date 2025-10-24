package org.transito_seguro.component.builder.sql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Componente especializado en proteger subconsultas y expresiones complejas.
 *
 * Responsabilidades:
 * - Proteger expresiones CASE WHEN...END (con anidamiento)
 * - Proteger subconsultas SELECT
 * - Proteger EXISTS
 * - Restaurar expresiones protegidas
 */
@Slf4j
@Component
public class SqlProtector {

    /**
     * Mapa temporal para almacenar expresiones protegidas.
     */
    private final ThreadLocal<Map<String, String>> protectedExpressions =
            ThreadLocal.withInitial(HashMap::new);

    private static final Pattern CASE_PATTERN = Pattern.compile(
            "\\bCASE\\b",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Protege subconsultas y expresiones complejas.
     *
     * @param sql Query SQL
     * @return SQL con placeholders
     */
    public String proteger(String sql) {
        limpiarMapa();

        try {
            // Proteger CASE (incluyendo anidados)
            sql = protegerExpresionesCase(sql);

            // Proteger EXISTS
            sql = protegerPatron(sql,
                    "EXISTS\\s*\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                    "EXISTS");

            // Proteger subconsultas SELECT
            sql = protegerPatron(sql,
                    "\\(\\s*SELECT[^()]*(?:\\([^()]*\\)[^()]*)*\\)",
                    "SUBSELECT");

            log.debug("Protegidas {} expresiones", protectedExpressions.get().size());

        } catch (Exception e) {
            log.error("Error protegiendo subconsultas", e);
            limpiarMapa();
        }

        return sql;
    }

    /**
     * Restaura expresiones protegidas.
     *
     * @param sql Query SQL con placeholders
     * @return SQL con expresiones restauradas
     */
    public String restaurar(String sql) {
        Map<String, String> mapa = protectedExpressions.get();

        if (!mapa.isEmpty()) {
            for (Map.Entry<String, String> entry : mapa.entrySet()) {
                sql = sql.replace(entry.getKey(), entry.getValue());
            }
            limpiarMapa();
        }

        return sql;
    }

    /**
     * Protege expresiones CASE WHEN...END respetando anidamiento.
     */
    private String protegerExpresionesCase(String sql) {
        Matcher matcher = CASE_PATTERN.matcher(sql);
        StringBuilder resultado = new StringBuilder();
        int lastEnd = 0;
        int contador = 0;

        while (matcher.find()) {
            resultado.append(sql, lastEnd, matcher.start());

            int inicio = matcher.start();
            int fin = encontrarEndDeCase(sql, matcher.end());

            if (fin > 0) {
                String expresionCase = sql.substring(inicio, fin);
                String placeholder = "___CASE_" + contador + "___";
                protectedExpressions.get().put(placeholder, expresionCase);
                resultado.append(placeholder);
                lastEnd = fin;
                contador++;
            } else {
                resultado.append(sql, inicio, matcher.end());
                lastEnd = matcher.end();
            }
        }

        resultado.append(sql.substring(lastEnd));
        return resultado.toString();
    }

    /**
     * Encuentra la posición del END que cierra un CASE.
     */
    private int encontrarEndDeCase(String sql, int inicioDesdeCase) {
        String upper = sql.toUpperCase();
        int nivelCase = 1;
        int pos = inicioDesdeCase;

        while (pos < sql.length() && nivelCase > 0) {
            int nextCase = upper.indexOf("CASE", pos);
            int nextEnd = upper.indexOf("END", pos);

            if (nextCase >= 0 && !esLimiteDePalabra(sql, nextCase, nextCase + 4)) {
                nextCase = -1;
            }
            if (nextEnd >= 0 && !esLimiteDePalabra(sql, nextEnd, nextEnd + 3)) {
                nextEnd = -1;
            }

            if (nextEnd < 0) {
                return -1;
            }

            if (nextCase >= 0 && nextCase < nextEnd) {
                nivelCase++;
                pos = nextCase + 4;
            } else {
                nivelCase--;
                if (nivelCase == 0) {
                    return nextEnd + 3;
                }
                pos = nextEnd + 3;
            }
        }

        return -1;
    }

    /**
     * Verifica si una posición es límite de palabra.
     */
    private boolean esLimiteDePalabra(String sql, int inicio, int fin) {
        if (inicio > 0 && Character.isLetterOrDigit(sql.charAt(inicio - 1))) {
            return false;
        }
        if (fin < sql.length() && Character.isLetterOrDigit(sql.charAt(fin))) {
            return false;
        }
        return true;
    }

    /**
     * Protege un patrón regex reemplazándolo con placeholders.
     */
    private String protegerPatron(String sql, String patronStr, String prefijo) {
        Pattern patron = Pattern.compile(patronStr, Pattern.CASE_INSENSITIVE);
        Matcher matcher = patron.matcher(sql);
        StringBuffer sb = new StringBuffer();
        int contador = 0;

        while (matcher.find()) {
            String placeholder = "___" + prefijo + "_" + contador + "___";
            protectedExpressions.get().put(placeholder, matcher.group(0));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(placeholder));
            contador++;
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Limpia el mapa de expresiones protegidas.
     */
    private void limpiarMapa() {
        protectedExpressions.get().clear();
    }

    /**
     * Verifica si hay expresiones protegidas pendientes.
     */
    public boolean tieneExpresionesProtegidas() {
        return !protectedExpressions.get().isEmpty();
    }
}