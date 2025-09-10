-- Query infracciones generadas reporte v2 (bruto)
SELECT
  TO_CHAR(DATE_TRUNC('month', i.fecha_mod), 'MM/YYYY') AS ultima_modificacion,
  TO_CHAR(DATE_TRUNC('month', i.fecha_infraccion), 'MM/YYYY') AS fecha_constatacion,
  TO_CHAR(DATE_TRUNC('month', elh.fecha_alta), 'MM/YYYY') AS mes_exportacion,
  c.descripcion AS municipio,
  ti.descripcion AS tipo_infraccion,
  pc.serie_equipo,
  -- estados
  SUM(CASE WHEN i.id_estado = 8   THEN 1 ELSE 0 END) AS pre_aprobada,
  SUM(CASE WHEN i.id_estado = 314 THEN 1 ELSE 0 END) AS filtrada_municipio,
  SUM(CASE WHEN i.id_estado = 340 THEN 1 ELSE 0 END) AS sin_datos,
  SUM(CASE WHEN i.id_estado = 15  THEN 1 ELSE 0 END) AS prescrita,
  SUM(CASE WHEN i.id_estado = 315  THEN 1 ELSE 0 END) AS filtrada,
  -- aprobadas = estado 10 o 20 o tuvo PDF generado
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
  -- PDF generado: igual a la query de Diego → COUNT(i.id)
  COUNT(elh.id) AS pdf_generado,
  -- total eventos
  (
    SUM(CASE WHEN i.id_estado = 8   THEN 1 ELSE 0 END) +
    SUM(CASE WHEN i.id_estado = 314 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN i.id_estado = 340 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN i.id_estado = 15  THEN 1 ELSE 0 END) +
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
JOIN tipo_infraccion ti ON ti.id = i.id_tipo_infra and ti.id in (1,3,4)
-- JOINS que hacen sumar los PDFs correctamente
LEFT JOIN lote_infraccion li ON li.id_infra = i.id
LEFT JOIN lote_infraccion_hd lih ON lih.id = li.id_lote_infra_hd AND lih.estado = 'C'
LEFT JOIN exportaciones_lote_detail eld ON eld.id_lote = lih.id
LEFT JOIN exportaciones_lote_header elh ON elh.id = eld.id_header
  AND elh.fecha_alta >= DATE '2018-01-01'
WHERE i.id_tipo_infra IN (1, 3, 4)
  AND i.fecha_mod >= DATE '2018-01-01'
  AND i.fecha_mod < NOW()
  --and i.fecha_infraccion >= DATE '2023-01-01'
  --and i.fecha_infraccion < NOW()
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY ultima_modificacion, fecha_constatacion, ti.descripcion, municipio, pc.serie_equipo;