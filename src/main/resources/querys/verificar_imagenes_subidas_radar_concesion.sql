--Verificar iamgenes subidas por radar / concesion especifica

SELECT
    c.id,
    c.provincia
    c.descripcion,
    TO_CHAR(i.fecha_alta, 'DD/MM/YYYY') AS fecha_alta,
    p.serie_equipo,
    i.packedfile,
    COUNT(*)
FROM
    infraccion i,
    concesion c,
    punto_control p
WHERE
    -- Filtros de fecha dinámicos
    AND (:fechaEspecifica IS NULL OR DATE(i.fecha_alta) = DATE(:fechaEspecifica))
    AND (:fechaInicio IS NULL OR i.fecha_alta >= :fechaInicio)
    AND (:fechaFin IS NULL OR i.fecha_alta <= :fechaFin)

    AND c.id = i.id_concesion
    AND p.id = i.id_punto_control
    -- Filtro de Tipo dispositivo
    AND (:tiposDispositivos IS NULL OR pc.id_tipo_dispositivo = ANY(CAST(:tiposDispositivos AS INTEGER[])))
GROUP BY
    c.id, c.descripcion, TO_CHAR(i.fecha_alta, 'DD/MM/YYYY'), p.serie_equipo, i.packedfile;