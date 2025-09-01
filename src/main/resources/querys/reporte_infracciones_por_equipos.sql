-- Reporte de infracciones por equipos

 SELECT
	c.descripcion as municipio,
	pc.serie_equipo,
	ti.descripcion AS tipo_infraccion,
	TO_CHAR(elh.fecha_alta, 'YYYY-MM') AS fecha_emision,
	elh.exporta_sacit as hacia_sacit,
	COUNT(i.id) AS cantidad
FROM infraccion i
	INNER JOIN punto_control pc on pc.id=i.id_punto_control
	INNER JOIN lote_infraccion li ON li.id_infra = i.id
	INNER JOIN lote_infraccion_hd lih ON lih.id = li.id_lote_infra_hd AND lih.estado = 'C'
	INNER JOIN exportaciones_lote_detail eld ON eld.id_lote = lih.id
	INNER JOIN exportaciones_lote_header elh ON elh.id = eld.id_header
	INNER JOIN concesion c ON c.id = i.id_concesion
	INNER JOIN tipo_infraccion ti ON ti.id = i.id_tipo_infra
WHERE elh.fecha_alta >= TO_DATE('2018-01-01', 'YYYY-MM-DD')
GROUP BY c.descripcion,pc.serie_equipo, ti.descripcion, TO_CHAR(elh.fecha_alta, 'YYYY-MM'), 5