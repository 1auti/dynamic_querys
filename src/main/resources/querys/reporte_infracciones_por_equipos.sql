-- reporte_infracciones_por_equipos.sql (VERSIÓN DINÁMICA)
-- Reporte de infracciones por equipos

SELECT
    c.descripcion AS municipio,
    pc.serie_equipo,
    ti.descripcion AS tipo_infraccion,
    TO_CHAR(elh.fecha_alta, 'YYYY-MM') AS fecha_emision,
    elh.exporta_sacit AS hacia_sacit,
    COUNT(i.id) AS cantidad
FROM infraccion i
    INNER JOIN punto_control pc ON pc.id = i.id_punto_control
    INNER JOIN lote_infraccion li ON li.id_infra = i.id
    INNER JOIN lote_infraccion_hd lih ON lih.id = li.id_lote_infra_hd AND lih.estado = 'C'
    INNER JOIN exportaciones_lote_detail eld ON eld.id_lote = lih.id
    INNER JOIN exportaciones_lote_header elh ON elh.id = eld.id_header
    INNER JOIN concesion c ON c.id = i.id_concesion
    INNER JOIN tipo_infraccion ti ON ti.id = i.id_tipo_infra
WHERE 1=1

    -- Filtros de fecha dinámicos (usando elh.fecha_alta)
    AND (:fechaEspecifica IS NULL OR DATE(elh.fecha_alta) = DATE(:fechaEspecifica))
    AND (:fechaInicio IS NULL OR elh.fecha_alta >= :fechaInicio)
    AND (:fechaFin IS NULL OR elh.fecha_alta <= :fechaFin)

    -- Filtros de equipos
   AND (:tipoEquipo IS NULL OR EXISTS (
       SELECT 1
       FROM punto_control pc_inner
       WHERE pc_inner.id = i.id_punto_control
       AND (
           SELECT bool_or(pc_inner.serie_equipo ILIKE '%' || patron || '%')
           FROM unnest(CAST(:tipoEquipo AS TEXT[])) AS patron
       )
   ))
    AND (:tiposDispositivos IS NULL OR pc.id_tipo_dispositivo = ANY(CAST(:tiposDispositivos AS INTEGER[])))
    AND (:lugares IS NULL OR pc.lugar = ANY(CAST(:lugares AS TEXT[])))

    -- Filtros de ubicación
    AND (:municipios IS NULL OR c.descripcion = ANY(CAST(:municipios AS TEXT[])))
    AND (:concesiones IS NULL OR c.id = ANY(CAST(:concesiones AS INTEGER[])))

    -- Filtros de infracciones
    AND (:tiposInfracciones IS NULL OR ti.id = ANY(CAST(:tiposInfracciones AS INTEGER[])))
    AND (:estadosInfracciones IS NULL OR i.id_estado = ANY(CAST(:estadosInfracciones AS INTEGER[])))

    -- Filtro de exportación a SACIT
    AND (:exportadoSacit IS NULL OR elh.exporta_sacit = :exportadoSacit)


GROUP BY c.descripcion, pc.serie_equipo, ti.descripcion, TO_CHAR(elh.fecha_alta, 'YYYY-MM'), elh.exporta_sacit
ORDER BY c.descripcion, pc.serie_equipo, TO_CHAR(elh.fecha_alta, 'YYYY-MM')
LIMIT COALESCE(:limite, 1000)
OFFSET COALESCE(:offset, 0)