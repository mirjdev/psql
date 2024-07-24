-- Операции над множествами
values (1,2)
union
values
      (1,2)
     ,(5,6)
;
--
"column1","column2"
5,6
1,2


values (1,2)
union all
values
      (1,2)
     ,(5,6)
;
-- не удаляет дубликаты
"column1","column2"
1,2
1,2
5,6


values
      (1,1)
     ,(3,3)
     ,(3,3)
intersect -- пересечения
values
      (3,3)
     ,(4,4)
     ,(1,1)
;
--
"column1","column2"
3,3
1,1


values
      (1)
     ,(2)
     ,(2)
except all  -- возвращает все строки, которые есть в результате первого запроса, но отсутствуют в результате второго. (Иногда это называют разницей двух запросов.) И здесь дублирующиеся строки отфильтровываются, если не указано ALL.
values
      (3)
     ,(4)
     ,(1)
;
--
"column1"
2
2


with
    t1(col1)  as (values (1),(2),(3)),
    t2(col2)  as (values (3),(4),(5))
select * from t1, t2;
--
"col1","col2"
1,3
1,4
1,5
2,3
2,4
2,5
3,3
3,4
3,5

with
    tbl1(col1)  as (values (1),(2),(3)),
    tbl2(col2)  as (values (3),(4),(5))
select * from tbl1 full join tbl2 on col1=col2;
--
"col1","col2"
1,null
2,null
3,3
null,5
null,4

--Пример 1,2,3 и 3,4,5 вывод должен быть 1,2,4,5 - симметрическая разность
-- Мой вариант
with
    tbl1(col1)  as (values (1),(2),(3)),
    tbl2(col2)  as (values (3),(4),(5))
select coalesce(col1, col2) as simetric_dif from tbl1 full join tbl2 on col1=col2 where col1 is null or col2 is null;
--
"simetric_dif"
1
2
5
4
-- Решение GPT
WITH first_set AS (
    SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS num
),
     second_set AS (
         SELECT * FROM UNNEST(ARRAY[3, 4, 5]) AS num
     )
SELECT * FROM first_set
WHERE num NOT IN (SELECT num FROM second_set)
UNION
SELECT * FROM second_set
WHERE num NOT IN (SELECT num FROM first_set);


