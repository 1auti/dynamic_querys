package org.transito_seguro.component.processor.execution;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.transito_seguro.component.processor.memory.MemoryMonitor;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.model.ContextoProcesamiento;
import org.transito_seguro.model.query.QueryStorage;
import org.transito_seguro.repository.impl.InfraccionesRepositoryImpl;
import org.transito_seguro.service.QueryRegistryService;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Ejecutor especializado en queries ESTÁNDAR (con paginación).
 *
 * Responsabilidades:
 * - Ejecutar queries con paginación OFFSET
 * - Manejar iteraciones y límites de seguridad
 * - Limpiar campos técnicos (row_id)
 *
 * @author Transito Seguro Team
 * @version 3.0 - Extraído de BatchProcessor
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StandardQueryExecutor {

    private final MemoryMonitor memoryMonitor;
    private final QueryRegistryService queryRegistryService;

    /**
     * Ejecuta query estándar usando paginación OFFSET.
     */
    public void ejecutar(
            InfraccionesRepositoryImpl repo,
            ParametrosFiltrosDTO filtros,
            String nombreQuery,
            String provincia,
            ContextoProcesamiento contexto,
            QueryStorage queryStorage) {

        log.info("🔄 {} - Iniciando paginación OFFSET", provincia);

        int procesados = 0;
        int iteracion = 0;
        final int batchSize = 10000;
        int offset = 0;

        Integer estimacion = queryStorage.getRegistrosEstimados();

        log.info("📊 Estimación para {}: {} registros",
                provincia, estimacion != null ? estimacion : "desconocido");

        while (true) {
            try {
                log.debug("🔹 {} - Iteración {}: offset={}, procesados={}/{}",
                        provincia, iteracion, offset, procesados,
                        estimacion != null ? estimacion : "?");

                long inicioIteracion = System.currentTimeMillis();

                // Crear filtros con OFFSET
                ParametrosFiltrosDTO filtrosLote = filtros.toBuilder()
                        .limite(batchSize)
                        .offset(offset)
                        .lastId(null)
                        .lastSerieEquipo(null)
                        .lastLugar(null)
                        .lastKeysetConsolidacion(null)
                        .build();

                // Ejecutar query
                List<Map<String, Object>> lote =
                        repo.ejecutarQueryConFiltros(nombreQuery, filtrosLote);

                long duracionIteracion = System.currentTimeMillis() - inicioIteracion;

                log.debug("✅ {} - Iteración {}: {} registros en {}ms",
                        provincia, iteracion,
                        lote != null ? lote.size() : 0,
                        duracionIteracion);

                // Verificar si hay datos
                if (lote == null || lote.isEmpty()) {
                    log.info("🏁 {} - Fin de datos en iteración {}", provincia, iteracion);
                    break;
                }

                // Procesar resultados (limpiar campos técnicos)
                List<Map<String, Object>> loteInmutable =
                        crearCopiasLimpias(lote, provincia);
                contexto.agregarResultados(loteInmutable);

                // Actualizar contadores
                procesados += lote.size();
                offset += batchSize;
                iteracion++;

                // Log de progreso
                if (estimacion != null && estimacion > 0) {
                    double progreso = (double) procesados / estimacion * 100;
                    log.debug("📊 {} - Progreso: {}/{} registros ({:.1f}%)",
                            provincia, procesados, estimacion, progreso);
                }

                // Condición de salida: lote incompleto
                if (lote.size() < batchSize) {
                    log.info("🏁 {} - Última página (lote incompleto: {})",
                            provincia, lote.size());
                    break;
                }

                // Límite de seguridad
                if (iteracion >= 100) {
                    log.warn("⚠️ {} - Límite de iteraciones alcanzado", provincia);
                    break;
                }

                // Pausa si memoria alta
                if (memoryMonitor.esMemoriaAlta()) {
                    memoryMonitor.pausarSiNecesario();
                }

            } catch (Exception e) {
                log.error("❌ {} - Error en iteración {}: {}",
                        provincia, iteracion, e.getMessage(), e);
                break;
            }
        }

        log.info("✅ {} - Paginación completada: {} registros en {} iteraciones | Memoria: {:.1f}%",
                provincia, procesados, iteracion, memoryMonitor.obtenerPorcentajeMemoriaUsada());
    }

    /**
     * Crea copias limpias SIN incluir row_id (campo técnico).
     */
    private List<Map<String, Object>> crearCopiasLimpias(
            List<Map<String, Object>> registros,
            String provincia) {

        return registros.stream()
                .map(registro -> {
                    Map<String, Object> copia = new HashMap<>();

                    // Copiar todos los campos EXCEPTO row_id y provincia
                    registro.entrySet().stream()
                            .filter(e -> !"row_id".equals(e.getKey()))
                            .filter(e -> !"provincia".equals(e.getKey()))
                            .forEach(e -> copia.put(e.getKey(), e.getValue()));

                    // Agregar provincia
                    copia.put("provincia", provincia);

                    return copia;
                })
                .collect(Collectors.toList());
    }
}