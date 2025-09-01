--Verificar iamgenes subidas por radar / concesion especifica

SELECT
    c.id,
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
    i.fecha_alta >= '2025-08-19'
    AND c.id = i.id_concesion
    AND p.id = i.id_punto_control
    AND p.id_tipo_dispositivo = 1
GROUP BY
    c.id, c.descripcion, TO_CHAR(i.fecha_alta, 'DD/MM/YYYY'), p.serie_equipo, i.packedfile;