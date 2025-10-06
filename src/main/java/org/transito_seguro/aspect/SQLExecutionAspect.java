package org.transito_seguro.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.transito_seguro.exception.SQLExecutionException;
import org.transito_seguro.utils.SQLExceptionParser;

@Aspect
@Component
@Slf4j
public class SQLExecutionAspect {

    /**
     * Intercepta métodos que ejecutan queries SQL
     */
    @Around("execution(* org.transito_seguro.repository..*.ejecutarQuery*(..)) && args(sql, queryName, ..)")
    public Object interceptSQLExecution(ProceedingJoinPoint joinPoint, String sql, String queryName) throws Throwable {
        try {
            return joinPoint.proceed();
        } catch (DataAccessException e) {
            // Extraer contexto adicional
            String contexto = extraerContexto(joinPoint);

            // Parsear y enriquecer la excepción
            SQLExecutionException enrichedException = SQLExceptionParser.parse(
                    e,
                    sql,
                    queryName,
                    contexto
            );

            // Log detallado
            log.error(enrichedException.getMessage(), enrichedException);

            // Re-lanzar la excepción enriquecida
            throw enrichedException;
        } catch (Exception e) {
            // Otras excepciones
            log.error("Error inesperado ejecutando query '{}': {}", queryName, e.getMessage(), e);
            throw e;
        }
    }

    private String extraerContexto(ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        StringBuilder contexto = new StringBuilder();

        // Buscar argumentos de contexto (provincia, etc.)
        for (Object arg : args) {
            if (arg != null && arg.toString().matches(".*[Pp]rovincia.*")) {
                contexto.append(arg.toString());
                break;
            }
        }

        return contexto.length() > 0 ? contexto.toString() : null;
    }
}

