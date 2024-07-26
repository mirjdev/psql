-- Другие примеры, лучшие практики

--- Представления, функции ---
------------------------------------------------------------------------------------------------------------------------
-- представление, в котором можно отлеживать процесс создания индексов
select *
from pg_stat_progress_create_index;

-- размер таблиц и индексов
SELECT
    TABLE_NAME,
    pg_size_pretty(table_size) AS table_size,
    pg_size_pretty(indexes_size) AS indexes_size,
    pg_size_pretty(total_size) AS total_size
FROM (
         SELECT
             TABLE_NAME,
             pg_table_size(TABLE_NAME) AS table_size,
             pg_indexes_size(TABLE_NAME) AS indexes_size,
             pg_total_relation_size(TABLE_NAME) AS total_size
         FROM (
                  SELECT ('"' || table_schema || '"."' || TABLE_NAME || '"') AS TABLE_NAME
                  FROM information_schema.tables
              ) AS all_tables
         ORDER BY total_size DESC
     ) AS pretty_sizes;

-- Одна строка для каждого серверного процесса c информацией по текущей активности процесса, такой как состояние и текущий запрос.
select * from pg_stat_activity where datname='psqlair';
-- Сколько подключений с одного адреса, к базе (кто жрет пул)
select client_addr, datname, now(), count(1) from pg_stat_activity where datname='psqlair' group by client_addr,datname;
-- Долгие запросы
select now() - query_start as time_execute, query, * from pg_stat_activity where state = 'active' order by 1 desc;
-- Для того, чтобы остановить конкретный запрос, выполним следующую команду, с указанием id процесса (pid):
SELECT pg_cancel_backend(:pid);
-- Для того, чтобы прекратить работу запроса, выполним:
SELECT pg_terminate_backend(:pid);

-- настройка сколько таблиц join будет пытаться оптимизировать, если больше используется порядок соединения будет как в запросе
join_collapse_limit=8

--Покажет текущие конфиги/настройки сервера
SHOW ALL;

-- не используемые индексы
SELECT * FROM pg_stat_user_indexes WHERE idx_scan = 0;

--- Индексы ---
------------------------------------------------------------------------------------------------------------------------

-- не используется индекс? пересчитай статистику
analyse verbose table;

-- создавать/удалять индексы лучше не блокируя таблицу (concurrently)
create index concurrently tasks_state_scheduled_dt_idx ON tasks USING btree (state, scheduled_dt) where complete_dt is null;
drop index concurrently tasks_state_scheduled_dt_idx;

-- Предпочтение следует отдать порядку state, scheduled_dt, в случае выборки задач по статусу и времени
-- USING btree (scheduled_dt, state) where complete_dt is null;
-- USING btree (state, scheduled_dt) where complete_dt is null ;

select id
from tasks
where complete_dt is null
  and scheduled_dt <= now()
  and state in ('SCHEDULED')
order by scheduled_dt
limit 100 for update skip locked;

-- Тип узла	Сущность	Стоимость	Строки	Время	Условие
-- Limit	[NULL]	0.57 - 125.52	100	0.121	[NULL]
-- LockRows	[NULL]	0.57 - 70895019.32	100	0.115	[NULL]
-- Index Scan	tasks	0.57 - 70327635.22	100	0.062	"(((tasks.state)::text = 'SCHEDULED'::text)
-- and (tasks.scheduled_dt <= now()))"

-- Триграммы
-- Данных 16 гигов + 11 гб индекс
create extension pg_trg;
create index on booking_jsonb using gist(cplx_booking gist_trgm_ops);
select * from booking_jsonb where cplx_booking_txt  like '%SIMONS14%' limit 10;
-- Тип узла	Сущность	Стоимость	Строки	Время	Условие
-- Limit	[NULL]	0.54 - 50.09	10	1.044	[NULL]
-- Index Scan	booking_jsonb	0.54 - 564915.61	10	1.041	(booking_jsonb.cplx_booking_txt ~~ '%SIMONS14%'::text)

-- рекурсия, числа Фибоначчи
with recursive fib(i,a,b) as (
    values(0,1,1)
    union all
    select i+1
         , greatest (a, b)
         , a + b
    from fib where i < 10
)
select * from fib;
--
|i  |a  |b  |
|---|---|---|
|0  |1  |1  |
|1  |1  |2  |
|2  |2  |3  |
|3  |3  |5  |
|4  |5  |8  |
|5  |8  |13 |
|6  |13 |21 |
|7  |21 |34 |
|8  |34 |55 |
|9  |55 |89 |
|10 |89 |144|

-- Оконные функции
-- https://postgrespro.ru/docs/postgrespro/10/functions-window
-- https://postgrespro.ru/docs/postgrespro/10/sql-expressions#SYNTAX-WINDOW-FUNCTIONS
-- https://habr.com/ru/companies/tensor/articles/785144/
with tbl (id, part, header, score) as (
    values
        (1,'part1','header', 10),
        (2,'part1','header', 20),
        (3,'part2','header', 9),
        (4,'part2','header', 19),
        (5,'part3','header', 50)
)

select *
     , row_number() over ()  as row_number	 -- порядковый номер строки вычисляется функцией
from  tbl order by id desc;
--
|id |part |header|score|row_number|
|---|-----|------|-----|----------|
|5  |part3|header|50   |5         |
|4  |part2|header|19   |4         |
|3  |part2|header|9    |3         |
|2  |part1|header|20   |2         |
|1  |part1|header|10   |1         |


------------------------------------------------------------
with tbl (id, part, header, score, deleted) as (
    values
        (1,'part1','header', 10, false),
        (2,'part1','header', 20, false),
        (3,'part2','header', 9, true),
        (4,'part2','header', 19, false),
        (5,'part3','header', 50, false)
)

select *
     , row_number() over ()  as row_number	 			   -- порядковый номер строки вычисляется функцией
     , sum(score) filter (where deleted is false)
    over(partition by part) as score_by_part               -- счет по партициям, накладывается фильтр не удаленные
     , avg(score) over window_all as avg_all 			   -- средний счет по всем
     , avg(score) over window_by_part as avg_by_part       -- средний счет в партиции
     , lag(score) over (order by score)	as privius_score   -- лаг, находим счет который был перед (важна сортировка)
     , lead(score) over (order by score)	as next_score  -- находим счет который следующий, к чему стремиться
from  tbl
window window_all as (),							       -- определение окон для переиспользования
       window_by_part as (partition by part)
order by score desc;
--
|id |part |header|score|deleted|row_number|score_by_part|avg_all|avg_by_part|privius_score|next_score|
|---|-----|------|-----|-------|----------|-------------|-------|-----------|-------------|----------|
|5  |part3|header|50   |false  |5         |50           |21,6   |50         |20           |          |
|2  |part1|header|20   |false  |4         |30           |21,6   |15         |19           |50        |
|4  |part2|header|19   |false  |3         |19           |21,6   |14         |10           |20        |
|1  |part1|header|10   |false  |2         |30           |21,6   |15         |9            |19        |
|3  |part2|header|9    |true   |1         |19           |21,6   |14         |             |10        |

------------------------------------------------------------
-- так же можно считать через баланс счета на момент, по таблице транзакций
with balance_change (id, change) as (
    values
        (1,10),
        (2,-5),
        (3,-3),
        (4,100),
        (5,-50)
)

select *
     , sum(change) over (order by id) as balance
from  balance_change
order by id
;
--
|id |change|balance|
|---|------|-------|
|1  |10    |10     |
|2  |-5    |5      |
|3  |-3    |2      |
|4  |100   |102    |
|5  |-50   |52     |

------------------------------------------------------------
-- RETURNING * вернет все значения, RETURNING accoint_id - вернет только этот столбец
insert into account (login) values ('adfsf') RETURNING *;



--- Статьи замечательных людей ---
------------------------------------------------------------------------------------------------------------------------
https://habr.com/ru/companies/postgrespro/articles/462877/ -- Егор Рогов postgrespro
https://habr.com/ru/users/Kilor/ -- Тензор