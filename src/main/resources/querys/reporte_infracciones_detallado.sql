-- Reporte detallado de infracciones con información completa del lote

SELECT
    x.fecha,
    x.provincia,
    x.municipio,
    x.tipo_infraccion,
    (CASE WHEN x.municipable = true THEN 'SI' ELSE 'NO' END) AS enviado_sacit,
    SUM(counter) AS total
FROM (
    SELECT
        TO_CHAR(elh.fecha_alta, 'DD/MM/YYYY') AS fecha,
        c.provincia AS provincia,
        c.descripcion AS municipio,
        ti.descripcion AS tipo_infraccion,
        elh.exporta_sacit AS municipable,
        counter
    FROM exportaciones_lote_header elh
    JOIN exportaciones_lote_detail eld ON eld.id_header = elh.id
    JOIN lote_infraccion l ON l.id_lote_infra_hd = eld.id_lote
    JOIN infraccion i ON i.id = l.id_infra
    JOIN concesion c ON c.id = elh.id_concesion
    JOIN tipo_infraccion ti ON ti.id = elh.id_tipo_infra
    WHERE 1=1
        -- Filtros de fecha dinámicos (CORREGIDOS)
        AND (:fechaEspecifica::DATE IS NULL OR DATE(elh.fecha_alta) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaFin::DATE)

        -- Filtro de concesiones dinámico (CORREGIDO)
        AND (:concesiones::INTEGER[] IS NULL OR elh.id_concesion = ANY(:concesiones::INTEGER[]))

        -- Filtros adicionales de infracciones (CORREGIDOS)
        AND (:tiposInfracciones::INTEGER[] IS NULL OR ti.id = ANY(:tiposInfracciones::INTEGER[]))
        AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)

        -- Filtros de municipio/provincia (CORREGIDOS)
        AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))

        -- Filtros de equipos con LIKE dinámico (CORREGIDOS)
        AND (:patronesEquipos::TEXT[] IS NULL OR EXISTS (
            SELECT 1
            FROM punto_control pc_inner
            WHERE pc_inner.id = i.id_punto_control
            AND (
                SELECT bool_or(pc_inner.serie_equipo ILIKE '%' || patron || '%')
                FROM unnest(:patronesEquipos::TEXT[]) AS patron
            )
        ))
) x
GROUP BY 1, 2, 3, 4, 5
ORDER BY TO_DATE(x.fecha, 'DD/MM/YYYY'), x.municipio
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);