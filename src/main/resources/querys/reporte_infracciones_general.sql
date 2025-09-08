
SELECT
    x.fecha,
    x.provincia,
    x.municipio,
    x.tipo_infraccion,
    (CASE WHEN x.exporta_sacit = true THEN 'SI' ELSE 'NO' END) AS enviado_sacit,
    SUM(counter) AS total
FROM (
    SELECT
        TO_CHAR(elh.fecha_alta, 'DD/MM/YYYY') AS fecha,
        c.provincia as provincia,
        c.descripcion AS municipio,
        ti.descripcion AS tipo_infraccion,
        elh.exporta_sacit AS exporta_sacit,
        counter
    FROM exportaciones_lote_header elh
    JOIN concesion c ON c.id = elh.id_concesion
    JOIN tipo_infraccion ti ON ti.id = elh.id_tipo_infra
    WHERE 1=1

        AND (:fechaEspecifica::DATE IS NULL OR DATE(elh.fecha_alta) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaFin::DATE)


        AND (:concesiones::INTEGER[] IS NULL OR elh.id_concesion = ANY(:concesiones::INTEGER[]))
        AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))


        AND (:tiposInfracciones::INTEGER[] IS NULL OR ti.id = ANY(:tiposInfracciones::INTEGER[]))


        AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)
) x
GROUP BY 1, 2, 3, 4, 5
ORDER BY TO_DATE(x.fecha, 'DD/MM/YYYY'), x.municipio
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);