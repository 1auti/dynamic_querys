SELECT
e.descripcion as estado,
count(i.*)
from infraccion i,
estado e
where e.id = i.id_Estado
group by 1;
