SELECT
    TO_CHAR(NOW(), 'DD/MM/YYYY') as fecha_reporte,
    x.provincia,
    x.municipio,
    COUNT(x.numero_doc) as total_sin_email
FROM (
    SELECT
        ii.numero_doc,
        c.provincia as provincia,
        c.descripcion as municipio
    FROM infraccion i
    JOIN infraccion_infractor ii ON ii.id = i.id_infraccion_infractor
    JOIN punto_control pc ON pc.id = i.id_punto_control
        AND pc.id_concesion = i.id_concesion
    JOIN concesion c ON c.id = pc.id_concesion
    WHERE 1=1
        -- Filtros de estado dinámicos (CORREGIDOS)
        AND (:estadosInfracciones::INTEGER[] IS NULL OR i.id_estado = ANY(:estadosInfracciones::INTEGER[]))
        -- Si no se especifica, usar el estado 20 por defecto
        AND (COALESCE(:estadosInfracciones::INTEGER[], ARRAY[20]) @> ARRAY[i.id_estado])

        -- Filtros de fecha dinámicos (CORREGIDOS) - usando fecha_infraccion
        AND (:fechaEspecifica::DATE IS NULL OR DATE(i.fecha_infraccion) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(i.fecha_infraccion) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(i.fecha_infraccion) <= :fechaFin::DATE)

        -- Filtros de ubicación (NUEVOS)
        AND (:municipios::TEXT[] IS NULL OR c.descripcion = ANY(:municipios::TEXT[]))
        AND (:provincias::TEXT[] IS NULL OR c.provincia = ANY(:provincias::TEXT[]))
        AND (:concesiones::INTEGER[] IS NULL OR c.id = ANY(:concesiones::INTEGER[]))

        -- Filtro de exportación SACIT (CORREGIDO y PARAMETRIZADO)
        AND (:exportadoSacit::BOOLEAN IS NULL OR pc.exporta_sacit = :exportadoSacit::BOOLEAN)
        -- Si no se especifica, usar false por defecto (comportamiento original)
        AND (COALESCE(:exportadoSacit::BOOLEAN, false) = pc.exporta_sacit)

        -- Filtro de email parametrizado (NUEVO - MUY IMPORTANTE)
        AND (
            :tieneEmail::BOOLEAN IS NULL OR
            (:tieneEmail::BOOLEAN = false AND (ii.email IS NULL OR ii.email = '')) OR
            (:tieneEmail::BOOLEAN = true AND ii.email IS NOT NULL AND ii.email != '')
        )
        -- Si no se especifica tieneEmail, buscar solo los que NO tienen email (comportamiento original)
        AND (COALESCE(:tieneEmail::BOOLEAN, false) = false AND (ii.email IS NULL OR ii.email = ''))

    GROUP BY ii.numero_doc, c.provincia, c.descripcion
) x
GROUP BY x.provincia, x.municipio
ORDER BY x.provincia, x.municipio
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);