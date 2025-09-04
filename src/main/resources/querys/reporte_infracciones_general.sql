-
-- Reporte general de infracciones por fecha, municipio y tipo DE BUENOS AIRES

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

        -- Filtros de fecha dinámicos (usando elh.fecha_alta)
        AND (:fechaEspecifica IS NULL OR DATE(elh.fecha_alta) = DATE(:fechaEspecifica))
        AND (:fechaInicio IS NULL OR elh.fecha_alta >= :fechaInicio)
        AND (:fechaFin IS NULL OR elh.fecha_alta <= :fechaFin)

        -- Filtros de concesión/municipio
        AND (:concesiones IS NULL OR elh.id_concesion = ANY(CAST(:concesiones AS INTEGER[])))
        AND (:municipios IS NULL OR c.descripcion = ANY(CAST(:municipios AS TEXT[])))

        -- Filtros de tipo de infracción
        AND (:tiposInfracciones IS NULL OR ti.id = ANY(CAST(:tiposInfracciones AS INTEGER[])))

        -- Filtro de exportación a SACIT
        AND (:exportadoSacit IS NULL OR elh.exporta_sacit = :exportadoSacit)

) x
GROUP BY 1, 2, 3, 4, 5
ORDER BY TO_DATE(x.fecha, 'DD/MM/YYYY'), x.municipio
LIMIT COALESCE(:limite, 1000)
OFFSET COALESCE(:offset, 0)