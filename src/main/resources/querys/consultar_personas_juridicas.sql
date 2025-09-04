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
    --Filtros de fecha
    AND (:fechaEspecifica IS NULL OR DATE(d.fecha_alta) = DATE (:fechaEspecifica))
    AND (:fechaInicio IS NULL OR DATE (d.fecha_alta) >= :fechaInicio)
    AND (:fechaFin IS NULL OR DATE (d.fecha_alta) <= :fechaFin)

    --Filtros de Email
    AND(
    :tieneEmail IS NULL OR
    (:tieneEmail = true AND dt.email IS NOT NULL AND dt.email != '') OR
    (:tieneEmail = false AND dt.email IS NULL AND dt.email = ''))

    --Filtro de Localidad
    AND(:provincia IS NULL OR dt.provincia = ANY(CAST(:provincias AS TEXT[])))
    AND(:partido IS NULL OR dt.partido = ANY(CAST(:partido AS TEXT[])))

    --Filtro Tipo de documento
    AND (:tipoDocumento IS NULL OR dt.tipo_documento = :tipoDocumento)

ORDER BY d.fecha_alta DESC, d.dominio ASC
LIMIT COALESCE(:limite, 1000)
OFFSET COALESCE(:offset, 0)
