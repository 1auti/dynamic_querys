-- Query infracciones generadas reporte v2 (bruto) - VERSIÓN DINÁMICA COMPLETA

SELECT
    TO_CHAR(DATE_TRUNC('month', i.fecha_mod), 'MM/YYYY') AS ultima_modificacion,
    TO_CHAR(DATE_TRUNC('month', i.fecha_infraccion), 'MM/YYYY') AS fecha_constatacion,
    TO_CHAR(DATE_TRUNC('month', elh.fecha_alta), 'MM/YYYY') AS mes_exportacion,
    c.provincia AS provincia,
    c.descripcion AS municipio,
    ti.descripcion AS tipo_infraccion,
    pc.serie_equipo,

    -- Estados específicos
    SUM(CASE WHEN i.id_estado = 8   THEN 1 ELSE 0 END) AS pre_aprobada,
    SUM(CASE WHEN i.id_estado = 314 THEN 1 ELSE 0 END) AS filtrada_municipio,
    SUM(CASE WHEN i.id_estado = 340 THEN 1 ELSE 0 END) AS sin_datos,
    SUM(CASE WHEN i.id_estado = 15  THEN 1 ELSE 0 END) AS prescrita,
    SUM(CASE WHEN i.id_estado = 315 THEN 1 ELSE 0 END) AS filtrada,

    -- Aprobadas (lógica compleja mantenida)
    SUM(
        CASE
            WHEN EXISTS (
                SELECT 1 FROM infraccion_estado ie
                WHERE ie.id_infra = i.id AND ie.id_estado_anterior IN (10, 20)
            )
            OR lih.estado = 'C' THEN 1
            ELSE 0
        END
    ) AS aprobada,

    -- PDF generado
    COUNT(elh.id) AS pdf_generado,

    -- Total eventos (suma de todos los estados)
    (
        SUM(CASE WHEN i.id_estado = 8   THEN 1 ELSE 0 END) +
        SUM(CASE WHEN i.id_estado = 314 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN i.id_estado = 340 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN i.id_estado = 15  THEN 1 ELSE 0 END) +
        SUM(CASE WHEN i.id_estado = 315 THEN 1 ELSE 0 END) +
        SUM(
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM infraccion_estado ie
                    WHERE ie.id_infra = i.id AND ie.id_estado_anterior IN (10, 20)
                )
                OR lih.estado = 'C' THEN 1
                ELSE 0
            END
        )
    ) AS total_eventos

FROM infraccion i
    JOIN punto_control pc ON pc.id = i.id_punto_control
    JOIN concesion c ON c.id = i.id_concesion
    JOIN tipo_infraccion ti ON ti.id = i.id_tipo_infra

    -- JOINS para PDFs (mantenidos como estaban)
    LEFT JOIN lote_infraccion li ON li.id_infra = i.id
    LEFT JOIN lote_infraccion_hd lih ON lih.id = li.id_lote_infra_hd AND lih.estado = 'C'
    LEFT JOIN exportaciones_lote_detail eld ON eld.id_lote = lih.id
    LEFT JOIN exportaciones_lote_header elh ON elh.id = eld.id_header

WHERE 1=1
    -- ============ FILTROS DINÁMICOS DE FECHA ============

    -- Filtros para fecha_mod (fecha de modificación)
    AND (:fechaInicio::DATE IS NULL OR DATE(i.fecha_mod) >= :fechaInicio::DATE)
    AND (:fechaFin::DATE IS NULL OR DATE(i.fecha_mod) <= :fechaFin::DATE)
    AND (:fechaEspecifica::DATE IS NULL OR DATE(i.fecha_mod) = :fechaEspecifica::DATE)

    -- Filtros adicionales para fecha_infraccion (fecha de constatación)
    AND (:fechaInfraccionInicio::DATE IS NULL OR DATE(i.fecha_infraccion) >= :fechaInfraccionInicio::DATE)
    AND (:fechaInfraccionFin::DATE IS NULL OR DATE(i.fecha_infraccion) <= :fechaInfraccionFin::DATE)

    -- Filtros para fecha de exportación
    AND (:fechaExportacionInicio::DATE IS NULL OR DATE(elh.fecha_alta) >= :fechaExportacionInicio::DATE)
    AND (:fechaExportacionFin::DATE IS NULL OR DATE(elh.fecha_alta) <= :fechaExportacionFin::DATE)

    -- ============ FILTROS DINÁMICOS DE UBICACIÓN ============
    AND (:provincias::TEXT[] IS NULL OR c.provincia = ANY(:provincias::TEXT[]))
    AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))
    AND (:concesiones::INTEGER[] IS NULL OR c.id = ANY(:concesiones::INTEGER[]))

    -- ============ FILTROS DINÁMICOS DE TIPOS ============
    AND (:tiposInfracciones::INTEGER[] IS NULL OR i.id_tipo_infra = ANY(:tiposInfracciones::INTEGER[]))
    -- Mantener la lógica original si no se especifica filtro
    AND (COALESCE(:tiposInfracciones::INTEGER[], ARRAY[1,3,4]) @> ARRAY[i.id_tipo_infra])

    -- ============ FILTROS DINÁMICOS DE ESTADOS ============
    AND (:estadosInfracciones::INTEGER[] IS NULL OR i.id_estado = ANY(:estadosInfracciones::INTEGER[]))

    -- ============ FILTROS DINÁMICOS DE EQUIPOS ============
    AND (:patronesEquipos::TEXT[] IS NULL OR EXISTS (
        SELECT 1
        FROM unnest(:patronesEquipos::TEXT[]) AS patron
        WHERE pc.serie_equipo ILIKE '%' || patron || '%'
    ))
    AND (:seriesEquiposExactas::TEXT[] IS NULL OR pc.serie_equipo = ANY(:seriesEquiposExactas::TEXT[]))

    -- ============ FILTROS DINÁMICOS ADICIONALES ============

    -- Filtro de exportación SACIT
    AND (:exportadoSacit::BOOLEAN IS NULL OR elh.exporta_sacit = :exportadoSacit::BOOLEAN)

    -- Filtro por estado de lote
    AND (:estadoLote::TEXT IS NULL OR lih.estado = :estadoLote::TEXT)

    -- Filtros de fecha base (mantener lógica original si no se especifica)
    AND (
        :fechaInicio::DATE IS NOT NULL OR
        COALESCE(DATE(i.fecha_mod), DATE '2018-01-01') >= DATE '2018-01-01'
    )
    AND (
        :fechaFin::DATE IS NOT NULL OR
        COALESCE(DATE(i.fecha_mod), NOW()::DATE) < NOW()::DATE
    )

GROUP BY
    TO_CHAR(DATE_TRUNC('month', i.fecha_mod), 'MM/YYYY'),
    TO_CHAR(DATE_TRUNC('month', i.fecha_infraccion), 'MM/YYYY'),
    TO_CHAR(DATE_TRUNC('month', elh.fecha_alta), 'MM/YYYY'),
    c.provincia,
    c.descripcion,
    ti.descripcion,
    pc.serie_equipo

ORDER BY
    TO_DATE(TO_CHAR(DATE_TRUNC('month', i.fecha_mod), 'MM/YYYY'), 'MM/YYYY') DESC,
    TO_DATE(TO_CHAR(DATE_TRUNC('month', i.fecha_infraccion), 'MM/YYYY'), 'MM/YYYY') DESC,
    ti.descripcion,
    c.provincia,
    c.descripcion,
    pc.serie_equipo

LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);