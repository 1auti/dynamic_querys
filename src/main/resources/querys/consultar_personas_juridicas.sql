
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
    dt.partido as "PARTIDO"
FROM dominios d
INNER JOIN dominio_titulares dt ON d.dominio = dt.dominio
WHERE 1=1
    AND dt.sexo IN ('J')  -- Solo personas jurídicas
    and d.fecha_alta  between '2024-01-03' and '2025-01-03'
    -- FILTRO_FECHA --
    -- FILTRO_PROVINCIA --
    -- FILTRO_MUNICIPIO --
    -- FILTRO_DOMINIO --
-- FILTRO_PAGINACION --;