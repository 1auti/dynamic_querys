-- Reporte de vehiculos, motos y formatos no validos por municipio

SELECT
    x.fecha_reporte,
    x.contexto,
    x.municipio,
    x.vehiculos,
    x.motos,
    x.formato_no_valido,
    (x.vehiculos + x.motos + x.formato_no_valido) as total
FROM (
    SELECT
        to_char(now(),'DD/MM/YYYY') as fecha_reporte,
        'PBA' as contexto,
        c.descripcion as municipio,
        count(case when dominio ~ '^(((A[A-H]{1})|(ZZ)[0-9]{3}[A-Z]{2})|([A-P|R-Z][A-Z]{2}[0-9]{3})|([X-Z][A-Z]{2}[0-9]{3}))$' then 1 end) as vehiculos,
        count(case when dominio ~ '^(([A][0-9]{3}[A-Z]{3})|([0-9]{3}[A-Z]{3}))$' then 1 end) as motos,
        count(case when dominio !~ '^(((A[A-H]{1})|(ZZ)[0-9]{3}[A-Z]{2})|([A-P|R-Z][A-Z]{2}[0-9]{3})|([X-Z][A-Z]{2}[0-9]{3}))|(([A][0-9]{3}[A-Z]{3})|([0-9]{3}[A-Z]{3}))$' then 1 end) as formato_no_valido
    FROM infraccion i, concesion c
    WHERE i.id_estado = 340
        AND i.fecha_infraccion > now() - interval '165 days'
        AND c.id = i.id_concesion
    GROUP BY 2,3
    ORDER BY 2,3
) x;