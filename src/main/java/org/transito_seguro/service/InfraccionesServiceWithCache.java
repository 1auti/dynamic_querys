package org.transito_seguro.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.transito_seguro.component.FormatoConverter;
import org.transito_seguro.dto.ConsultaQueryDTO;
import org.transito_seguro.dto.ParametrosFiltrosDTO;

import javax.xml.bind.ValidationException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("infraccionesServiceWithCache")
public class InfraccionesServiceWithCache extends InfraccionesServiceEnhanced {

    @Autowired
    private QueryCacheService queryCacheService;

    @Autowired
    private FormatoConverter formatoConverter;

    @Override
    public Object consultarInfracciones(ConsultaQueryDTO consulta, String nombreQuery) throws ValidationException {
        ParametrosFiltrosDTO filtros = consulta.getParametrosFiltros();

        // Intentar obtener desde cache si es cacheable
        if (queryCacheService.isCacheable(filtros)) {
            List<Map<String, Object>> cachedResult = queryCacheService.getCachedResult(nombreQuery, filtros);
            if (cachedResult != null) {
                log.info("Resultado obtenido desde cache: {} registros", cachedResult.size());
                String formato = consulta.getFormato() != null ? consulta.getFormato() : "json";
                return formatoConverter.convertir(cachedResult, formato);
            }
        }

        // Si no está en cache, ejecutar consulta normal
        Object resultado = super.consultarInfracciones(consulta, nombreQuery);

        // Si el resultado es una lista (síncrono), almacenar en cache
        if (resultado instanceof String || resultado instanceof byte[]) {
            // Es un resultado formateado, no lo cacheamos así
            // TODO: Podríamos cachear el resultado raw antes del formateo
        }

        return resultado;
    }

    // Método para cachear resultados desde el BatchProcessor
    public void cachearResultadoSiAplica(String nombreQuery, ParametrosFiltrosDTO filtros, List<Map<String, Object>> resultado) {
        if (queryCacheService.isCacheable(filtros)) {
            queryCacheService.cacheResult(nombreQuery, filtros, resultado);
        }
    }
}