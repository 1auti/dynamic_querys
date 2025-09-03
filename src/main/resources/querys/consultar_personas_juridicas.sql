-- consultar_personas_juridicas_dinamica.sql
-- Obtener datos completos de dominios y titulares para personas jurídicas
-- VERSION DINAMICA compatible con ParametrosProcessor

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
    
    -- FILTROS DINAMICOS DE FECHA --
    -- FILTRO_FECHA --
    
    -- FILTROS DE UBICACION --  
    -- FILTRO_PROVINCIA --
    -- FILTRO_MUNICIPIO --
    
    -- FILTROS DE DOMINIOS Y VEHICULOS --
    -- FILTRO_DOMINIO --
    
    -- FILTROS ADICIONALES ESPECIFICOS --
    -- Los filtros de equipos e infracciones no aplican para esta query
    -- pero mantenemos la estructura para consistencia

-- ORDENAMIENTO Y PAGINACION (CRITICO PARA BATCHING) --
ORDER BY d.fecha_alta DESC, d.dominio  -- Orden consistente para paginación
-- FILTRO_PAGINACION --