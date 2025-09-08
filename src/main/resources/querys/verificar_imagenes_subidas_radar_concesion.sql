--Verificar iamgenes subidas por radar / concesion especifica

SELECT
    c.id,
    c.provincia,
    c.descripcion,
    TO_CHAR(i.fecha_alta, 'DD/MM/YYYY') AS fecha_alta,
    p.serie_equipo,
    i.packedfile,
    COUNT(*)
FROM
    infraccion i,
    concesion c,
    punto_control p
WHERE 1=1
    -- Filtros de fecha dinámicos (CORREGIDOS)
    AND (:fechaEspecifica::DATE IS NULL OR DATE(i.fecha_alta) = :fechaEspecifica::DATE)
    AND (:fechaInicio::DATE IS NULL OR DATE(i.fecha_alta) >= :fechaInicio::DATE)
    AND (:fechaFin::DATE IS NULL OR DATE(i.fecha_alta) <= :fechaFin::DATE)

    AND c.id = i.id_concesion
    AND p.id = i.id_punto_control

    -- Filtro de Tipo dispositivo (CORREGIDO)
    AND (:tiposDispositivos::INTEGER[] IS NULL OR p.id_tipo_dispositivo = ANY(:tiposDispositivos::INTEGER[]))

GROUP BY
    c.id, c.descripcion, TO_CHAR(i.fecha_alta, 'DD/MM/YYYY'), p.serie_equipo, i.packedfile
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);