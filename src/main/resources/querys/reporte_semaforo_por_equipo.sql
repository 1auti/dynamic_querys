-- reporte_semaforo_por_equipo.sql (VERSIÓN DINÁMICA)
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

        -- Filtros de fecha dinámicos (usando elh.fecha_alta)
        AND (:fechaEspecifica IS NULL OR DATE(elh.fecha_alta) = DATE(:fechaEspecifica))
        AND (:fechaInicio IS NULL OR elh.fecha_alta >= :fechaInicio)
        AND (:fechaFin IS NULL OR elh.fecha_alta <= :fechaFin)

        -- Filtros de serie de equipo con LIKE para SE% o VLR%
        AND (
            :filtrarPorTipoEquipo IS NULL OR
            :filtrarPorTipoEquipo = false OR
            (
                (:incluirSE IS NULL OR :incluirSE = false OR pc.serie_equipo ILIKE 'SE%') OR
                (:incluirVLR IS NULL OR :incluirVLR = false OR pc.serie_equipo ILIKE 'VLR%')
            )
        )

        -- Filtro adicional para series específicas (si las necesitas exactas)
        AND (:seriesEquiposExactas IS NULL OR pc.serie_equipo = ANY(CAST(:seriesEquiposExactas AS TEXT[])))

        -- Asegurar que serie_equipo no sea null
        AND pc.serie_equipo IS NOT NULL

        -- Filtro de exportación a SACIT
        AND (:exportadoSacit IS NULL OR elh.exporta_sacit = :exportadoSacit)

        -- Filtros adicionales de ubicación
        AND (:municipios IS NULL OR c.descripcion = ANY(CAST(:municipios AS TEXT[])))
        AND (:concesiones IS NULL OR c.id = ANY(CAST(:concesiones AS INTEGER[])))

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ORDER BY 1
) x
GROUP BY 1, 2, 3, 4, 5
ORDER BY TO_DATE(x.fecha, 'DD/MM/YYYY'), x.municipio, x.serie_equipo
LIMIT COALESCE(:limite, 1000)
OFFSET COALESCE(:offset, 0)