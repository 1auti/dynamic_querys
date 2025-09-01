-- Reporte detallado de infracciones con informacion completa del lote

SELECT
    x.fecha,
    x.contexto,
    x.municipio,
    x.tipo_infraccion,
    (case when x.municipable = true then 'SI' else 'NO' end) as enviado_sacit,
    sum(counter) as total
FROM (
    SELECT
        to_char(elh.fecha_alta,'DD/MM/YYYY') as fecha,
        'PBA' as contexto,
        c.descripcion as municipio,
        ti.descripcion as tipo_infraccion,
        elh.exporta_sacit as municipable,
        counter
    FROM exportaciones_lote_header elh
    JOIN exportaciones_lote_detail eld on eld.id_header = elh.id
    JOIN lote_infraccion l on l.id_lote_infra_hd = eld.id_lote
    JOIN infraccion i on i.id = l.id_infra
    JOIN concesion c on c.id = elh.id_concesion
    JOIN tipo_infraccion ti on ti.id = elh.id_tipo_infra
    WHERE elh.fecha_alta >= '2025-07-01'
        AND elh.fecha_alta < '2025-08-01'
        AND elh.id_concesion in (22,19,21)
) x
GROUP BY 1,2,3,4,5
ORDER BY to_date(x.fecha, 'DD/MM/YYYY'), x.municipio;