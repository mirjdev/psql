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


--- Статьи замечательных людей ---
------------------------------------------------------------------------------------------------------------------------
https://habr.com/ru/companies/postgrespro/articles/462877/ -- Егор Рогов postgrespro
https://habr.com/ru/users/Kilor/ -- Тензор