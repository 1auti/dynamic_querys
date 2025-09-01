-- Reporte general de infracciones por fecha, municipio y tipo

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
    JOIN concesion c on c.id = elh.id_concesion
    JOIN tipo_infraccion ti on ti.id = elh.id_tipo_infra
    WHERE elh.fecha_alta > '2025-08-22'
) x
GROUP BY 1,2,3,4,5
ORDER BY to_date(x.fecha, 'DD/MM/YYYY'), x.municipio;
