-- Contar infracciones sin email por municipio

SELECT
    to_char(now(),'DD/MM/YYYY') as fecha_reporte,
    x.provincia,
    x.municipio,
    count(x.numero_doc) as total_sin_email
FROM (
    SELECT
        ii.numero_doc,
        c.provincia as provincia,
        c.descripcion as municipio
    FROM infraccion i
    JOIN infraccion_infractor ii on ii.id = i.id_infraccion_infractor
    JOIN punto_control pc on pc.id = i.id_punto_control
        AND pc.id_concesion = i.id_concesion
    JOIN concesion c on c.id = pc.id_concesion
    WHERE i.id_estado in (20)
        AND pc.exporta_sacit = false
        AND (ii.email is null or ii.email = '')
    GROUP BY ii.numero_doc, c.descripcion
) x
GROUP BY x.municipio
ORDER BY x.municipio;