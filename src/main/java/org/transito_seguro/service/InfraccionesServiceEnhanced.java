package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;
import org.transito_seguro.dto.TaskInfo;


import javax.xml.bind.ValidationException;

/**
 * Versión mejorada del InfraccionesService que incluye:
 * - Detección automática de consultas grandes
 * - Delegación a procesamiento asíncrono cuando sea necesario
 * - Fallback a procesamiento síncrono para consultas pequeñas
 */
@Slf4j
@Service("infraccionesServiceEnhanced")
public class InfraccionesServiceEnhanced extends InfraccionesService {

    @Autowired
    private TaskService taskService;

    private static final int UMBRAL_ASINCRONO = 1000; // Si se estima >1000 registros, usar async

    /**
     * Método inteligente que decide entre síncrono vs asíncrono
     */
    public Object consultarInfraccionesInteligente(ConsultaQueryDTO consulta) throws ValidationException {

        // 1. Estimar tamaño de la consulta
        int registrosEstimados = estimarTamanoConsulta(consulta.getParametrosFiltros());

        log.info("Consulta estimada: ~{} registros", registrosEstimados);

        // 2. Decidir procesamiento
        if (registrosEstimados > UMBRAL_ASINCRONO) {
            log.info("Consulta grande detectada, usando procesamiento asíncrono");
            return iniciarConsultaAsincrona(consulta, "consulta-automatica");
        } else {
            log.info("Consulta pequeña, usando procesamiento síncrono");
            return super.consultarInfracciones(consulta); // Llamar al método padre
        }
    }

    /**
     * Inicia una consulta asíncrona y devuelve información de la tarea
     */
    public TaskInfo iniciarConsultaAsincrona(ConsultaQueryDTO consulta, String tipoConsulta) {
        return taskService.iniciarConsultaAsincrona(consulta, tipoConsulta);
    }

    /**
     * Estima el tamaño de una consulta basado en filtros
     */
    private int estimarTamanoConsulta(ParametrosFiltrosDTO filtros) {
        int baseEstimacion = 5000; // Por provincia por defecto

        // Ajustar según provincias
        if (filtros.debeConsultarTodasLasBDS()) {
            baseEstimacion *= 6; // ~6 provincias promedio
        } else if (filtros.getBaseDatos() != null) {
            baseEstimacion *= filtros.getBaseDatos().size();
        }

        // Ajustar según filtros temporales
        if (filtros.debeUsarFechaEspecifica()) {
            baseEstimacion = baseEstimacion / 10; // Mucho menos registros en un día
        } else if (filtros.tieneRangoFechas()) {
            // Calcular días aproximados
            if (filtros.getFechaInicio() != null && filtros.getFechaFin() != null) {
                long dias = Math.abs(filtros.getFechaFin().getTime() - filtros.getFechaInicio().getTime())
                        / (24 * 60 * 60 * 1000);
                if (dias < 30) {
                    baseEstimacion = baseEstimacion / 3; // Menos registros en rangos cortos
                }
            }
        }

        // Ajustar según otros filtros específicos
        if (filtros.getDominios() != null && !filtros.getDominios().isEmpty()) {
            baseEstimacion = filtros.getDominios().size() * 5; // Muy específico
        }

        if (filtros.getNumerosDocumentos() != null && !filtros.getNumerosDocumentos().isEmpty()) {
            baseEstimacion = filtros.getNumerosDocumentos().size() * 3; // Muy específico
        }

        return Math.max(baseEstimacion, 0);
    }

    // Delegar métodos específicos al TaskService cuando sea apropiado

    @Override
    public Object consultarPersonasJuridicas(ConsultaQueryDTO consulta) throws ValidationException {
        if (estimarTamanoConsulta(consulta.getParametrosFiltros()) > UMBRAL_ASINCRONO) {
            return taskService.iniciarConsultaAsincrona(consulta, "personas-juridicas");
        }
        return super.consultarPersonasJuridicas(consulta);
    }

    @Override
    public Object consultarReporteGeneral(ConsultaQueryDTO consulta) throws ValidationException {
        if (estimarTamanoConsulta(consulta.getParametrosFiltros()) > UMBRAL_ASINCRONO) {
            return taskService.iniciarConsultaAsincrona(consulta, "reporte-general");
        }
        return super.consultarReporteGeneral(consulta);
    }

    @Override
    public Object consultarInfraccionesPorEquipos(ConsultaQueryDTO consulta) throws ValidationException {
        if (estimarTamanoConsulta(consulta.getParametrosFiltros()) > UMBRAL_ASINCRONO) {
            return taskService.iniciarConsultaAsincrona(consulta, "infracciones-por-equipos");
        }
        return super.consultarInfraccionesPorEquipos(consulta);
    }
}