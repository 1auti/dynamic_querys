-- REPORTE SIMPLE DE CANTIDAD DE INFRACCIONES POR ESTADO (SIN AGRUPACIÓN POR MES)

SELECT 
    e.descripcion AS estado,
    c.provincia AS provincia, 
    c.descripcion AS municipio,
    COUNT(i.id) AS cantidad_infracciones
FROM infraccion i
    INNER JOIN estado e ON e.id = i.id_estado
    INNER JOIN concesion c ON c.id = i.id_concesion
    LEFT JOIN tipo_infraccion ti ON ti.id = i.id_tipo_infra
WHERE 1=1
    -- Filtros de fecha dinámicos
    AND (:fechaEspecifica::DATE IS NULL OR DATE(i.fecha_alta) = :fechaEspecifica::DATE)
    AND (:fechaInicio::DATE IS NULL OR DATE(i.fecha_alta) >= :fechaInicio::DATE)
    AND (:fechaFin::DATE IS NULL OR DATE(i.fecha_alta) <= :fechaFin::DATE)

    -- Filtros de estado dinámicos
    AND (:estadosInfracciones::INTEGER[] IS NULL OR i.id_estado = ANY(:estadosInfracciones::INTEGER[]))

    -- Filtros de ubicación dinámicos
    AND (:provincias::TEXT[] IS NULL OR c.provincia = ANY(:provincias::TEXT[]))
    AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))
    AND (:concesiones::INTEGER[] IS NULL OR c.id = ANY(:concesiones::INTEGER[]))

    -- Filtros de tipo de infracción
    AND (:tiposInfracciones::INTEGER[] IS NULL OR i.id_tipo_infra = ANY(:tiposInfracciones::INTEGER[]))

GROUP BY 
    e.descripcion,
    c.provincia, 
    c.descripcion

ORDER BY 
    c.provincia,
    c.descripcion, 
    e.descripcion

LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);