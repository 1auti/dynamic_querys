
-- Reporte de vehículos, motos y formatos no válidos por municipio

SELECT
    x.fecha_reporte,
    x.contexto,
    x.municipio,
    x.vehiculos,
    x.motos,
    x.formato_no_valido,
    (x.vehiculos + x.motos + x.formato_no_valido) AS total
FROM (
    SELECT
        COALESCE(TO_CHAR(:fechaReporte::DATE, 'DD/MM/YYYY'), TO_CHAR(NOW(), 'DD/MM/YYYY')) AS fecha_reporte,
        c.provincia AS contexto,
        c.descripcion AS municipio,

        -- Conteo de vehículos (regex mejorado para más formatos)
        COUNT(CASE WHEN i.dominio ~ '^(((A[A-H]{1})|(ZZ))[0-9]{3}[A-Z]{2}|([A-P]|[R-Z])[A-Z]{2}[0-9]{3}|[X-Z][A-Z]{2}[0-9]{3}|[A-Z]{2}[0-9]{3}[A-Z]{2})$' THEN 1 END) AS vehiculos,

        -- Conteo de motos (regex mejorado)
        COUNT(CASE WHEN i.dominio ~ '^([A]?[0-9]{3}[A-Z]{3})$' THEN 1 END) AS motos,

        -- Conteo de formatos no válidos (resto que no coincide con ningún patrón)
        COUNT(CASE WHEN i.dominio !~ '^(((A[A-H]{1})|(ZZ))[0-9]{3}[A-Z]{2}|([A-P]|[R-Z])[A-Z]{2}[0-9]{3}|[X-Z][A-Z]{2}[0-9]{3}|[A-Z]{2}[0-9]{3}[A-Z]{2}|([A]?[0-9]{3}[A-Z]{3}))$' THEN 1 END) AS formato_no_valido

    FROM infraccion i
    JOIN concesion c ON c.id = i.id_concesion
    WHERE 1=1
        -- Filtros de estado dinámicos (CORREGIDOS)
        AND (:estadosInfracciones::INTEGER[] IS NULL OR i.id_estado = ANY(:estadosInfracciones::INTEGER[]))

        -- Filtros de fecha dinámicos (CORREGIDOS)
        AND (:fechaEspecifica::DATE IS NULL OR DATE(i.fecha_infraccion) = :fechaEspecifica::DATE)
        AND (:fechaInicio::DATE IS NULL OR DATE(i.fecha_infraccion) >= :fechaInicio::DATE)
        AND (:fechaFin::DATE IS NULL OR DATE(i.fecha_infraccion) <= :fechaFin::DATE)

    GROUP BY c.provincia, c.descripcion
    ORDER BY c.provincia, c.descripcion
) x
ORDER BY x.contexto, x.municipio
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0);
