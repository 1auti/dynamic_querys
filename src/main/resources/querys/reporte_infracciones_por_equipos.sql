
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
    -- Filtros de fecha dinámicos (CORREGIDOS)
    AND (:fechaEspecifica::DATE IS NULL OR DATE(elh.fecha_alta) = :fechaEspecifica::DATE)
    AND (:fechaInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaInicio::DATE)
    AND (:fechaFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaFin::DATE)

    -- Filtros de equipos (CORREGIDOS)
    AND (:tipoEquipo::TEXT[] IS NULL OR EXISTS (
        SELECT 1
        FROM punto_control pc_inner
        WHERE pc_inner.id = i.id_punto_control
        AND (
            SELECT bool_or(pc_inner.serie_equipo ILIKE '%' || patron || '%')
            FROM unnest(:tipoEquipo::TEXT[]) AS patron
        )
    ))
    AND (:tiposDispositivos::INTEGER[] IS NULL OR pc.id_tipo_dispositivo = ANY(:tiposDispositivos::INTEGER[]))
    AND (:lugares::TEXT[] IS NULL OR pc.lugar = ANY(:lugares::TEXT[]))

    -- Filtros de ubicación (CORREGIDOS)
    AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))
    AND (:concesiones::INTEGER[] IS NULL OR c.id = ANY(:concesiones::INTEGER[]))

    -- Filtros de infracciones (CORREGIDOS)
    AND (:tiposInfracciones::INTEGER[] IS NULL OR ti.id = ANY(:tiposInfracciones::INTEGER[]))
    AND (:estadosInfracciones::INTEGER[] IS NULL OR i.id_estado = ANY(:estadosInfracciones::INTEGER[]))

    -- Filtro de exportación a SACIT (CORREGIDO)
    AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)

GROUP BY c.descripcion, pc.serie_equipo, ti.descripcion, TO_CHAR(elh.fecha_alta, 'YYYY-MM'), elh.exporta_sacit
ORDER BY c.descripcion, pc.serie_equipo, TO_CHAR(elh.fecha_alta, 'YYYY-MM')
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);