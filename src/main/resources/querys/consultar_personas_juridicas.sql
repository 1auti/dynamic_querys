-- consultar_personas_juridicas.sql (VERSIÓN CORREGIDA CON PLACEHOLDERS ESPECÍFICOS)
-- Obtener datos completos de dominios y titulares para personas jurídicas

SELECT
    d.dominio as "DOMINIO",
    'CUIT' as "TIPO_DOCUMENTO",
    dt.numero_documento as "NRO_DOCUMENTO",
    dt.cuit as "NRO_CUIT",
    dt.propietario as "PROPIETARIO_APELLIDO",
    '' as "PROPIETARIO_NOMBRE",
    dt.fecha_titularidad as "FECHA_TITULARIDAD",
    dt.domicilio as "CALLE",
    dt.numero as "NUMERO",
    dt.piso as "PISO",
    dt.dpto as "DEPARTAMENTO",
    dt.cp as "CP",
    dt.localidad as "LOCALIDAD",
    dt.provincia as "PROVINCIA",
    dt.telefono as "TELEFONO",
    dt.celular as "CELULAR",
    dt.email as "EMAIL",
    d.marca as "MARCA",
    d.modelo as "MODELO",
    d.tipo_vehiculo as "TIPO",
    dt.partido as "PARTIDO",
    d.fecha_alta as "FECHA_ALTA_DOMINIO",
    CASE
        WHEN dt.email IS NOT NULL AND dt.email != '' THEN 'SI'
        ELSE 'NO'
    END as "TIENE_EMAIL",
    CASE
        WHEN dt.sexo = 'J' THEN 'JURIDICA'
        ELSE 'FISICA'
    END as "TIPO_PERSONA"
FROM dominios d
INNER JOIN dominio_titulares dt ON d.dominio = dt.dominio
WHERE 1=1
    AND dt.sexo IN ('J')  -- Solo personas jurídicas (requerimiento base)

    -- *** PLACEHOLDERS ESPECÍFICOS (REEMPLAZAR LOS GENÉRICOS) *** --
    -- FILTRO_FECHA_DOMINIOS --      -- Usará d.fecha_alta en lugar de elh.fecha_alta
    -- FILTRO_PROVINCIA_TITULARES --  -- Usará dt.provincia en lugar de c.descripcion
    -- FILTRO_MUNICIPIO_TITULARES --  -- Usará dt.localidad para municipios
    -- FILTRO_DOMINIOS_ESPECIFICOS -- -- Usará d.dominio, dt.numero_documento, dt.email, etc.

ORDER BY d.fecha_alta DESC, d.dominio ASC
-- FILTRO_PAGINACION --