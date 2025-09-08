-- Reporte de infracciones de radar fijo por equipo y ubicacion

SELECT
    x.fecha,
    x.contexto,
    x.municipio,
    x.serie_equipo,
    x.lugar,
    sum(case when x.id_tipo_infraccion = 1 then counter else 0 end) velocidad_radar_fijo,
    sum(counter) as total
FROM (
    SELECT
        to_char(elh.fecha_alta,'DD/MM/YYYY') as fecha,
        c.provincia as contexto,
        c.descripcion as municipio,
        pc.serie_equipo,
        pc.lugar,
        ti.id as id_tipo_infraccion,
        ti.descripcion as tipo_infraccion,
        (case when ti.id = 1 then counter else 0 end) velocidad_radar_fijo,
        counter
    FROM exportaciones_lote_header elh
    JOIN exportaciones_lote_detail eld on eld.id_header = elh.id
    JOIN lote_infraccion l on l.id_lote_infra_hd = eld.id_lote
    JOIN infraccion i on i.id = l.id_infra
    JOIN punto_control pc on pc.id = i.id_punto_control
    JOIN concesion c on c.id = elh.id_concesion
    JOIN tipo_infraccion ti on ti.id = elh.id_tipo_infra and ti.id in (1)
    WHERE 1=1
        -- Filtros de fecha dinámicos (CORREGIDOS)
        AND (:fechaEspecifica::DATE IS NULL OR DATE(elh.fecha_alta) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaFin::DATE)

        -- Filtro de exportación a SACIT (CORREGIDO)
        AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)

    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY 1
) x
GROUP BY 1,2,3,4,5
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);
