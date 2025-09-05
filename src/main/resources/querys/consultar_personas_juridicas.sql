SELECT
    d.dominio AS "DOMINIO",
    'CUIT' AS "TIPO_DOCUMENTO",
    dt.numero_documento AS "NRO_DOCUMENTO",
    dt.cuit AS "NRO_CUIT",
    dt.propietario AS "PROPIETARIO_APELLIDO",
    '' AS "PROPIETARIO_NOMBRE",
    dt.fecha_titularidad AS "FECHA_TITULARIDAD",
    dt.domicilio AS "CALLE",
    dt.numero AS "NUMERO",
    dt.piso AS "PISO",
    dt.dpto AS "DEPARTAMENTO",
    dt.cp AS "CP",
    dt.localidad AS "LOCALIDAD",
    dt.provincia AS "PROVINCIA",
    dt.telefono AS "TELEFONO",
    dt.celular AS "CELULAR",
    dt.email AS "EMAIL",
    d.marca AS "MARCA",
    d.modelo AS "MODELO",
    d.tipo_vehiculo AS "TIPO",
    dt.partido AS "PARTIDO"
FROM
    dominios d
JOIN
    dominio_titulares dt ON d.dominio = dt.dominio
WHERE
    dt.sexo IN ('J')

    -- Filtros de fecha (CORREGIDOS)
    AND (:fechaEspecifica::DATE IS NULL OR DATE(d.fecha_alta) = :fechaEspecifica::DATE)
    AND (:fechaInicio::DATE IS NULL OR DATE(d.fecha_alta) >= :fechaInicio::DATE)
    AND (:fechaFin::DATE IS NULL OR DATE(d.fecha_alta) <= :fechaFin::DATE)

    -- Filtros de Email (CORREGIDO - lógica simplificada)
    AND (
        :tieneEmail::BOOLEAN IS NULL OR
        (:tieneEmail::BOOLEAN = true AND dt.email IS NOT NULL AND dt.email != '') OR
        (:tieneEmail::BOOLEAN = false AND (dt.email IS NULL OR dt.email = ''))
    )

    -- Filtro de Localidad (CORREGIDOS)
    AND (:provincias::TEXT[] IS NULL OR dt.provincia = ANY(:provincias::TEXT[]))
    AND (:partido::TEXT[] IS NULL OR dt.partido = ANY(:partido::TEXT[]))

    -- Filtro Tipo de documento (CORREGIDO)
    AND (:tipoDocumento::VARCHAR IS NULL OR dt.tipo_documento = :tipoDocumento::VARCHAR)

ORDER BY d.fecha_alta DESC, d.dominio ASC
LIMIT COALESCE(:limite::INTEGER, 1000)
OFFSET COALESCE(:offset::INTEGER, 0)