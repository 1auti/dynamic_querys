select e.id, e.descripcion, count(*)
from infraccion i, estado e
where i.id_estado not in (1,310,316,666)
and e.id=i.id_estado
and i.fecha_infraccion>='2025-03-17'
group by 1;