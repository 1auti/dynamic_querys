-- Reporte de infracciones de luz roja y senda peatonal por equipo

SELECT
    x.fecha,
    x.contexto,
    x.municipio,
    x.serie_equipo,
    x.lugar,
    sum(case when x.id_tipo_infraccion = 3 then counter else 0 end) luz_roja,
    sum(case when x.id_tipo_infraccion = 4 then counter else 0 end) senda,
    sum(counter) as total
FROM (
    SELECT
        to_char(elh.fecha_alta,'DD/MM/YYYY') as fecha,
        'PBA' as contexto,
        c.descripcion as municipio,
        pc.serie_equipo,
        pc.lugar,
        ti.id as id_tipo_infraccion,
        ti.descripcion as tipo_infraccion,
        (case when ti.id = 3 then counter else 0 end) luz_roja,
        (case when ti.id = 4 then counter else 0 end) senda,
        counter
    FROM exportaciones_lote_header elh
    JOIN exportaciones_lote_detail eld on eld.id_header = elh.id
    JOIN lote_infraccion l on l.id_lote_infra_hd = eld.id_lote
    JOIN infraccion i on i.id = l.id_infra
    JOIN punto_control pc on pc.id = i.id_punto_control
    JOIN concesion c on c.id = elh.id_concesion
    JOIN tipo_infraccion ti on ti.id = elh.id_tipo_infra and ti.id in (3,4)
    WHERE elh.fecha_alta >= '2025-07-01'
        AND elh.fecha_alta < '2025-08-01'
        AND pc.serie_equipo like 'SE%'
        AND pc.serie_equipo is not null
        AND elh.exporta_sacit = true
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    ORDER BY 1
) x
GROUP BY 1,2,3,4,5;