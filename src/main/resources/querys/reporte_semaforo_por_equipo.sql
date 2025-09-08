
-- Reporte de infracciones de luz roja y senda peatonal por equipo

SELECT
    x.fecha,
    x.contexto,
    x.municipio,
    x.serie_equipo,
    x.lugar,
    SUM(CASE WHEN x.id_tipo_infraccion = 3 THEN counter ELSE 0 END) AS luz_roja,
    SUM(CASE WHEN x.id_tipo_infraccion = 4 THEN counter ELSE 0 END) AS senda,
    SUM(counter) AS total
FROM (
    SELECT
        TO_CHAR(elh.fecha_alta, 'DD/MM/YYYY') AS fecha,
        c.provincia AS contexto,
        c.descripcion AS municipio,
        pc.serie_equipo,
        pc.lugar,
        ti.id AS id_tipo_infraccion,
        ti.descripcion AS tipo_infraccion,
        (CASE WHEN ti.id = 3 THEN counter ELSE 0 END) AS luz_roja,
        (CASE WHEN ti.id = 4 THEN counter ELSE 0 END) AS senda,
        counter
    FROM exportaciones_lote_header elh
    JOIN exportaciones_lote_detail eld ON eld.id_header = elh.id
    JOIN lote_infraccion l ON l.id_lote_infra_hd = eld.id_lote
    JOIN infraccion i ON i.id = l.id_infra
    JOIN punto_control pc ON pc.id = i.id_punto_control
    JOIN concesion c ON c.id = elh.id_concesion
    JOIN tipo_infraccion ti ON ti.id = elh.id_tipo_infra AND ti.id IN (3, 4)
    WHERE 1=1
        -- Filtros de fecha dinámicos (CORREGIDOS)
        AND (:fechaEspecifica::DATE IS NULL OR DATE(elh.fecha_alta) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaFin::DATE)

        -- Filtros de serie de equipo con LIKE (CORREGIDOS)
        AND (
            :filtrarPorTipoEquipo::BOOLEAN IS NULL OR
            :filtrarPorTipoEquipo::BOOLEAN = false OR
            (
                (:incluirSE::BOOLEAN IS NULL OR :incluirSE::BOOLEAN = false OR pc.serie_equipo ILIKE 'SE%') OR
                (:incluirVLR::BOOLEAN IS NULL OR :incluirVLR::BOOLEAN = false OR pc.serie_equipo ILIKE 'VLR%')
            )
        )

        -- Filtro adicional para series específicas (CORREGIDO)
        AND (:seriesEquiposExactas::TEXT[] IS NULL OR pc.serie_equipo = ANY(:seriesEquiposExactas::TEXT[]))

        -- Asegurar que serie_equipo no sea null
        AND pc.serie_equipo IS NOT NULL

        -- Filtro de exportación a SACIT (CORREGIDO)
        AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)

        -- Filtros adicionales de ubicación (CORREGIDOS)
        AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))
        AND (:concesiones::INTEGER[] IS NULL OR c.id = ANY(:concesiones::INTEGER[]))

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ORDER BY 1
) x
GROUP BY 1, 2, 3, 4, 5
ORDER BY TO_DATE(x.fecha, 'DD/MM/YYYY'), x.municipio, x.serie_equipo
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);