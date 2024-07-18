-- Оптимизация запросов PostgreSQL - 2022 Домбровская Г., Новиков Б., Бейликова А.
-- скачать  базу можно https://drive.google.com/drive/folders/13F7M80Kf_somnjb-mTYAnh1hW1Y_g4kJ?usp=sharing
-- psql -d psqlair -f "C:\postgres_air.sql"
-- 263 Страница поиск по jsonb 


--создаем индекс для поиска по jsonb, обычный json или текст не поддерживается
--На таблицу 7 гб, индекс весит 2 
CREATE INDEX idxgin ON booking_jsonb USING GIN (cplx_booking);

--Bitmap Index Scan idxgin 127.525 --1к 0.035
SELECT * FROM booking_jsonb WHERE cplx_booking @@ '$.**.departure_airport_code == "ORD" && $.**.arrival_airport_code == "JFK"'

--Bitmap Index Scan 47.681 --1к 0.017
SELECT * FROM booking_jsonb WHERE cplx_booking @@  '$.**.flight_no == "3015"'
  
 -- seq scan
 SELECT * FROM booking_jsonb WHERE cplx_booking -> 'account_id' = '207984' and booking_id='3642';
 
--Bitmap Index Scan 0.103
 SELECT * FROM booking_jsonb WHERE cplx_booking @@ '$.account_id == 207984' limit 1;

--Bitmap Heap Scan idxgin 0.086 --1к 0.027
SELECT * FROM booking_jsonb WHERE cplx_booking @@ '$.*.booking_leg_id == 16711840';

-- поиск по btree 0.043  --1к 0.024
create index booking_jsonb_email_idx on booking_jsonb using btree((cplx_booking -> 'email'));
SELECT * FROM booking_jsonb WHERE cplx_booking -> 'email' = '"CROWLEY621@magic.email"' limit 1; -- текст  должен быть в '""'

--Index Scan booking_jsonb_booking_id_idx 0.025 -1к -0.019
create index booking_jsonb_booking_id_idx on booking_jsonb using btree((cplx_booking -> 'booking_id'));
SELECT * FROM booking_jsonb WHERE cplx_booking -> 'booking_id' = '1672' limit 1; 

-- Полнотекстовый поиск по текстовому полю 
ALTER TABLE postgres_air.booking_jsonb ADD cplx_booking_text text NULL;
update booking_jsonb set cplx_booking_text=cplx_booking;
-- 9,7 размер -> 20 
/*
Updated Rows	5643216
Query	update booking_jsonb set cplx_booking_text=cplx_booking
Start time	Thu Jul 18 11:52:44 MSK 2024
Finish time	Thu Jul 18 12:37:26 MSK 2024
*/
  
 
 
ALTER TABLE postgres_air.booking_jsonb ADD cplx_booking_text_1k text NULL;
update booking_jsonb set cplx_booking_text_1k=cplx_booking where booking_id  < 1000;
--1k = 670085
--100 = 64634, id 3 = 634
SELECT sum(pg_column_size(cplx_booking_text_1k)) FROM booking_jsonb where booking_id  = 3;
--100 = 77676, id 3 = 754
SELECT sum(pg_column_size(cplx_booking)) FROM booking_jsonb where booking_id  = 3;

truncate booking_jsonb;


----------------------------
--- на 1 к записей
 ALTER TABLE postgres_air.booking_jsonb
            ADD doc_tsv_for_gist tsvector
            GENERATED ALWAYS AS
            (to_tsvector('pg_catalog.english', cplx_booking)) 
            stored;

-- GIST
CREATE INDEX ts_cplx_booking_text_gist_idx ON booking_jsonb USING gist(doc_tsv_for_gist);
select * from booking_jsonb where doc_tsv_for_gist @@ to_tsquery('VIVIAN');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	booking_jsonb	4.43 - 23.38	7	0.080	[NULL]
Bitmap Index Scan	ts_cplx_booking_text_gist_idx	0.00 - 4.43	7	0.055	(booking_jsonb.doc_tsv_for_gist @@ to_tsquery('VIVIAN'::text))

-- GIN 
 ALTER TABLE postgres_air.booking_jsonb
            ADD doc_tsv_for_gin tsvector
            GENERATED ALWAYS AS
            (to_tsvector('pg_catalog.english', cplx_booking)) 
            stored;
           
CREATE INDEX ts_cplx_booking_text_gin_idx ON booking_jsonb USING gin(doc_tsv_for_gin); 
select * from booking_jsonb where doc_tsv_for_gin @@ to_tsquery('VIVIAN');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	booking_jsonb	12.29 - 31.24	7	0.029	[NULL]
Bitmap Index Scan	ts_cplx_booking_text_gin_idx	0.00 - 12.29	7	0.015	(booking_jsonb.doc_tsv_for_gin @@ to_tsquery('VIVIAN'::text))

--- GIN без отдельного столбца
CREATE INDEX ts_cplx_booking_text_gin_idx ON booking_jsonb USING gin(to_tsvector('english', cplx_booking_text_1k)); 
select * from booking_jsonb where to_tsvector('english', cplx_booking_text_1k) @@ to_tsquery('english','VIVIAN');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	booking_jsonb	12.04 - 31.68	7	0.023	[NULL]
Bitmap Index Scan	ts_cplx_booking_text_gin_idx	0.00 - 12.04	7	0.011	"(to_tsvector('english'::regconfig,booking_jsonb.cplx_booking_text_1k) @@ '''vivian'''::tsquery)"

select * from booking_jsonb where cplx_booking_text_1k like '%VIVIAN%';
----------------------------

--- создание для запросов выше
CREATE TYPE boarding_pass_record AS (
 boarding_pass_id int,
 booking_leg_id int,
 flight_no text,
 departure_airport text,
 arrival_airport text,
 last_name text,
 first_name text,
 seat text,
 boarding_time timestamptz
);

CREATE TYPE flight_record AS (
 flight_id int,
 flight_no text,
 departure_airport_code text,
 departure_airport_name text,
 arrival_airport_code text,
 arrival_airport_name text,
 scheduled_departure timestamptz,
 scheduled_arrival timestamptz
);

CREATE TYPE booking_leg_record AS (
 booking_leg_id int,
 leg_num int,
 booking_id int,
 flight flight_record,
 boarding_passes boarding_pass_record[]
);


-- упрощенный тип для сегмента бронирования
CREATE TYPE booking_leg_record_2 AS (
 booking_leg_id integer,
 leg_num integer,
 booking_id integer,
 flight flight_record
);

CREATE TYPE postgres_air.passenger_record AS (
	passenger_id int4,
	booking_id int4,
	passenger_no int4,
	first_name text,
	last_name text);

-- упрощенный тип для бронирования
CREATE TYPE booking_record_2 AS (
 booking_id integer,
 booking_ref text,
 booking_name text,
 email text,
 account_id integer,
 booking_legs booking_leg_record_2[],
 passengers passenger_record[]
);
-- таблица
CREATE TABLE booking_jsonb AS
SELECT b.booking_id,
 to_jsonb( row (
 b.booking_id,
 b.booking_ref,
 b.booking_name,
 b.email,
 b.account_id,
 ls.legs,
 ps.passengers
 )::booking_record_2
 ) AS cplx_booking
 FROM booking b
 JOIN ( SELECT booking_id,
 array_agg( row (
 booking_leg_id,
 leg_num,
 booking_id,
 row (
 f.flight_id,
 flight_no,
 departure_airport,
 dep.airport_name,
 arrival_airport,
 arv.airport_name,
 scheduled_departure,
 scheduled_arrival
 )::flight_record
 )::booking_leg_record_2) legs
 FROM booking_leg l
 JOIN flight f ON f.flight_id = l.flight_id
 JOIN airport dep ON dep.airport_code = f.departure_airport
 JOIN airport arv ON arv.airport_code = f.arrival_airport
 GROUP BY booking_id
 ) ls ON b.booking_id = ls.booking_id
 JOIN ( SELECT booking_id,
 array_agg( row(
 passenger_id,
 booking_id,
 passenger_no,
 last_name,
 first_name
 )::passenger_record) AS passengers
 FROM passenger
 GROUP BY booking_id
 ) ps ON ls.booking_id = ps.booking_id
 and ps.booking_id < 1000
;
---------------------------------------------------------------
-- Поиск по индексам = like 

https://habr.com/ru/articles/169751/
хинты для постгреса, как ОТДЕЛЬНОЕ расширение с версии 9,2

Без индекса
select * from account a  where first_name = 'Андрей';
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Gather	[NULL]	1000.00 - 5584.15	50	[NULL]	[NULL]
Parallel Seq Scan	account	0.00 - 4579.15	29	[NULL]	(a.first_name = 'Андрей'::text)

--- Создание обычного b-tree (создается text_ops), где не будет использования индекса по like
### CREATE INDEX account_first_name_lower_pattern  ON account (lower(first_name));
select * from account a  where lower(first_name) = lower('Андрей');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	account	18.39 - 2255.45	1	[NULL]	[NULL]
Bitmap Index Scan	account_first_name_lower_pattern	0.00 - 18.07	1	[NULL]	(lower(a.first_name) = 'андрей'::text)

select * from account a  where lower(first_name) like lower('андре%');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Gather	[NULL]	1000.00 - 6086.29	1	[NULL]	[NULL]
Parallel Seq Scan	account	0.00 - 4957.59	1	[NULL]	(lower(a.first_name) ~~ 'андре%'::text)

--- Создание функционального индекса для поиска по like
### DROP INDEX postgres_air.account_first_name_lower_pattern;
### CREATE INDEX account_first_name_lower_pattern  ON account (lower(first_name) text_pattern_ops);
select * from account a  where lower(first_name) = lower('Андрей');
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	account	18.39 - 2255.45	1	[NULL]	[NULL]
Bitmap Index Scan	account_first_name_lower_pattern	0.00 - 18.07	1	[NULL]	(lower(a.first_name) = 'андрей'::text)

select * from account a  where lower(first_name) like lower('андре%'); !!! lower(first_name) важно указать столбец через функцию, иначе индекс не будет использоваться
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Bitmap Heap Scan	account	21.61 - 2258.66	1	[NULL]	(lower(a.first_name) ~~ 'андре%'::text)
Bitmap Index Scan	account_first_name_lower_pattern	0.00 - 21.29	1	[NULL]	"((lower(a.first_name) ~>=~ 'андре'::text)
and (lower(a.first_name) ~<~ 'андрж'::text))"

select * from account a  where first_name like lower('Андре%'); - столбец в запросе без функции lowerи индекс не используется
Тип узла	Сущность	Стоимость	Строки	Время	Условие
Gather	[NULL]	1000.00 - 5581.06	0	[NULL]	[NULL]
Parallel Seq Scan	account	0.00 - 4579.16	0	[NULL]	(a.first_name ~~ 'андре%'::text)



explain SELECT f.flight_no,
 f.actual_departure,
 count(passenger_id) passengers
 FROM flight f
 JOIN booking_leg bl ON bl.flight_id = f.flight_id
 JOIN passenger p ON p.booking_id=bl.booking_id
 WHERE f.departure_airport = 'JFK'
 AND f.arrival_airport = 'ORD'
 AND f.actual_departure BETWEEN '2020-08-14' AND '2020-08-15'
GROUP BY f.flight_id, f.actual_departure;

SELECT f.flight_no,
 f.actual_departure,
 count(passenger_id) passengers
 FROM flight f
 JOIN booking_leg bl ON bl.flight_id = f.flight_id
 JOIN passenger p ON p.booking_id=bl.booking_id
 WHERE f.departure_airport = 'JFK'
 AND f.arrival_airport = 'ORD'
 AND f.actual_departure BETWEEN '2020-08-14' AND '2020-08-15'
GROUP BY f.flight_id, f.actual_departure;

SELECT d.airport_code AS departure_airport,
 a.airport_code AS arrival_airport
 FROM airport a,
 airport d;
 
SELECT f.flight_no,
 f.scheduled_departure,
 boarding_time,
 p.last_name,
 p.first_name,
 bp.update_ts as pass_issued,
 ff.level
 FROM flight f
 JOIN booking_leg bl ON bl.flight_id = f.flight_id
 JOIN passenger p ON p.booking_id = bl.booking_id
 JOIN account a ON a.account_id = p.account_id
 JOIN boarding_pass bp ON bp.passenger_id = p.passenger_id
 LEFT OUTER JOIN frequent_flyer ff ON ff.frequent_flyer_id = a.frequent_flyer_id
 WHERE f.departure_airport = 'JFK'
 AND f.arrival_airport = 'ORD'
 AND f.scheduled_departure BETWEEN '2020-08-05' AND '2020-08-07'
 
 SELECT f.flight_no,
 f.scheduled_departure,
 model,
 count(passenger_id) passengers
 FROM flight f
 JOIN booking_leg bl ON bl.flight_id = f.flight_id
 JOIN passenger p ON p.booking_id=bl.booking_id
 JOIN aircraft ac ON ac.code=f.aircraft_code
 WHERE f.departure_airport = 'JFK'
 AND f.arrival_airport = 'ORD'
 AND f.scheduled_departure BETWEEN '2020-08-14' AND '2020-08-16'
 GROUP BY 1,2,3
 
CREATE INDEX account_login ON account (login);
CREATE INDEX account_login_lower_pattern  ON account (lower(login) text_pattern_ops);
CREATE INDEX passenger_last_name ON passenger (last_name);
CREATE INDEX boarding_pass_passenger_id ON boarding_pass (passenger_id);
CREATE INDEX passenger_last_name_lower_pattern ON passenger (lower(last_name) text_pattern_ops);
CREATE INDEX passenger_booking_id ON passenger (booking_id);
CREATE INDEX booking_account_id ON booking (account_id);

CREATE INDEX account_first_name_lower_pattern  ON account (lower(first_name) text_pattern_ops);
CREATE INDEX account_first_name_lower_pattern  ON account (lower(first_name) text_pattern_ops);

CREATE INDEX account_first_name_lower_pattern  ON account (first_name);

select * from account a  where lower(login) like'%aabv%';

select * from account a  where first_name = 'AIDEN';
select * from account a  where first_name like 'AIDEN%';

select * from account a  where login = 'smith';
select * from account a  where lower(first_name) = lower('Андрей');
select * from account a  where lower (first_name) ilike 'Андре%';
select * from account a  where lower(first_name) = 'андрей';
select count (1) from account a ;

SELECT b.account_id,
 a.login,
 p.last_name,
 p.first_name
 FROM passenger p
 JOIN booking b USING(booking_id)
 JOIN account a ON a.account_id = b.account_id
 WHERE lower(p.last_name) = 'smith'
 AND lower(login) LIKE 'smith%'
 
 
CREATE INDEX frequent_fl_last_name_lower_pattern ON frequent_flyer (lower(last_name) text_pattern_ops);
CREATE INDEX frequent_fl_last_name_lower ON frequent_flyer (lower(last_name));

SELECT a.account_id,
 a.login,
 f.last_name,
 f.first_name,
 count(*) AS num_bookings
 FROM frequent_flyer f
 JOIN account a USING(frequent_flyer_id)
 JOIN booking b USING(account_id)
 WHERE lower(f.last_name) = 'smith'
 AND lower(login) LIKE 'smith%'
 GROUP BY 1,2,3,4

select * from pg_stat_all_indexes;

SELECT 
    schemaname,
    pg_stat_all_indexes.relname AS table,
    pg_class.relname AS index,
    pg_total_relation_size(oid) AS size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM  pg_class
JOIN pg_stat_all_indexes ON pg_stat_all_indexes.indexrelname = pg_class.relname
WHERE  
    relkind =('i')
ORDER BY size desc

explain analyze SELECT * FROM flight f WHERE EXISTS
 (SELECT flight_id FROM booking_leg WHERE flight_id = f.flight_id);

--              Heap Fetches: 508776
explain analyze SELECT * FROM flight WHERE flight_id IN
 (SELECT flight_id FROM booking_leg);
 
explain analyze SELECT departure_airport,
 booking_id,
 is_returning
 FROM booking_leg bl
 JOIN flight f USING (flight_id)
 WHERE departure_airport IN
 (SELECT airport_code FROM airport WHERE iso_country = 'US');

CREATE INDEX booking_update_ts ON booking (update_ts);



explain analyze SELECT departure_airport, booking_id, is_returning
 FROM booking_leg bl
 JOIN flight f USING (flight_id)
 WHERE departure_airport IN
 (SELECT airport_code FROM airport WHERE iso_country = 'US')
 AND bl.booking_id IN
 (SELECT booking_id FROM booking WHERE update_ts > '2020-07-01');

 


--174400
explain analyze  SELECT count(1) FROM flight f WHERE NOT EXISTS
 (SELECT flight_id FROM booking_leg WHERE flight_id = f.flight_id);

explain analyze SELECT count(1) from (
SELECT flight_id FROM flight f
EXCEPT
SELECT flight_id FROM booking_leg) as a;

SELECT * FROM flight f WHERE NOT EXISTS
 (SELECT flight_id FROM booking_leg WHERE flight_id = f.flight_id);

SELECT *
 FROM flight f
 JOIN (SELECT DISTINCT flight_id FROM booking_leg) bl
 USING (flight_id)
 
 SELECT flight_id
 FROM flight f
 JOIN (SELECT DISTINCT flight_id FROM booking_leg) bl USING (flight_id)

explain analyze  SELECT COUNT(DISTINCT f.flight_id)
FROM flight f
LEFT JOIN booking_leg bl ON f.flight_id = bl.flight_id
WHERE bl.flight_id IS NULL;

--explain analyze  SELECT * FROM flight WHERE flight_id NOT IN (SELECT flight_id FROM booking_leg);

SELECT flight_id,
 avg_price,
 num_passengers
 FROM (
 SELECT bl.flight_id,
 departure_airport,
 (avg(price))::numeric (7,2) AS avg_price,
 count(DISTINCT passenger_id) AS num_passengers
 FROM booking b
 JOIN booking_leg bl USING (booking_id)
 JOIN flight f USING (flight_id)
 JOIN passenger p USING (booking_id)
 GROUP BY 1,2
 )a 
 WHERE departure_airport = 'ORD';




SELECT city,
 date_trunc('month', scheduled_departure) AS month,
 count(*) passengers
 FROM airport a
 JOIN flight f ON airport_code = departure_airport
 JOIN booking_leg l ON f.flight_id =l.flight_id
 JOIN boarding_pass b ON b.booking_leg_id = l.booking_leg_id
 GROUP BY 1,2
 ORDER BY 3 desc
 
 SELECT city,
 date_trunc('month', scheduled_departure),
 sum(passengers) passengers
 FROM airport a
 JOIN flight f ON airport_code = departure_airport
 JOIN (
 SELECT flight_id, count(*) passengers
 FROM booking_leg l
 JOIN boarding_pass b USING (booking_leg_id)
 GROUP BY flight_id
 ) cnt USING (flight_id)
 GROUP BY 1,2
 ORDER BY 3 DESC

 SELECT CASE
 WHEN actual_departure > scheduled_departure + interval '1 hour'
 THEN 'Late group 1'
 ELSE 'Late group 2'
 END AS grouping,
 flight_id,
 count(*) AS num_passengers
 FROM boarding_pass bp
 JOIN booking_leg bl USING (booking_leg_id)
 JOIN booking b USING (booking_id)
 JOIN flight f USING (flight_id)
 WHERE departure_airport = 'FRA'
 AND actual_departure > '2020-07-01'
 AND (
 ( actual_departure > scheduled_departure + interval '30 minute'
 AND actual_departure <= scheduled_departure + interval '1 hour'
 )
 OR
 ( actual_departure>scheduled_departure + interval '1 hour'
 AND bp.update_ts > scheduled_departure + interval '30 minute'
 )
 )
 GROUP BY 1,2
 
 SELECT 'Late group 1' AS grouping,
 flight_id,
 count(*) AS num_passengers
 FROM boarding_pass bp
 JOIN booking_leg bl USING (booking_leg_id)
 JOIN booking b USING (booking_id)
 JOIN flight f USING (flight_id)
 WHERE departure_airport = 'FRA'
 AND actual_departure > scheduled_departure + interval '1 hour'
 AND bp.update_ts > scheduled_departure + interval '30 minutes'
 AND actual_departure > '2020-07-01'
 GROUP BY 1,2
UNION ALL
SELECT 'Late group 2' AS grouping,
 flight_id,
 count(*) AS num_passengers
 FROM boarding_pass bp
 JOIN booking_leg bl USING(booking_leg_id)
 JOIN booking b USING (booking_id)
 JOIN flight f USING (flight_id)
 WHERE departure_airport = 'FRA'
 AND actual_departure > scheduled_departure + interval '30 minute'
 AND actual_departure <= scheduled_departure + interval '1 hour'
 AND actual_departure > '2020-07-01'
 GROUP BY 1,2

 
 create table custom_field (
custom_field_id serial,
passenger_id int,
custom_field_name text,
custom_field_value text);
alter table custom_field
add constraint custom_field_pk primary key (custom_field_id);

do $$
declare v_rec record;
begin
for v_rec in (select passenger_id from passenger)
loop
insert into custom_field (passenger_id, 
						 custom_field_name,
                          custom_field_value)
						  values
						  (v_rec.passenger_id,
						  'passport_num',
						  ((random()*1000000000000)::bigint)::text);
end loop;
end;
$$;



do $$
declare v_rec record;
v_days int;
begin
for v_rec in (select passenger_id from passenger)
loop
v_days:=(random()*5000)::int;
insert into custom_field (passenger_id, 
						 custom_field_name,
                          custom_field_value)
						  values
						  (v_rec.passenger_id,
						  'passport_exp_date',
						  (('2020-08-18'::date + v_days*interval '1 day')::date)::text);
end loop;
end;
$$;

do $$
declare v_rec record;
v_country text;
begin
for v_rec in (select passenger_id from passenger)
loop
v_country:=case mod (v_rec.passenger_id, 7)
when 0 then 'Mordor'
when 1 then 'Narnia'
When 2 then 'Shambhala'
when 3 then 'Shire'
when 4 then 'Narnia'
when 5 then 'Shire'
when 6 then 'Narnia'
end ;

insert into custom_field (passenger_id, 
						 custom_field_name,
                          custom_field_value)
						  values
						  (v_rec.passenger_id,
						  'passport_country',
						  v_country);
end loop;
end;
$$;







SELECT first_name,
 last_name,
 pn.custom_field_value AS passport_num,
 pe.custom_field_value AS passport_exp_date,
 pc.custom_field_value AS passport_country
 FROM passenger p
 JOIN custom_field pn ON pn.passenger_id = p.passenger_id
 AND pn.custom_field_name = 'passport_num'
 JOIN custom_field pe ON pe.passenger_id = p.passenger_id
 AND pe.custom_field_name = 'passport_exp_date'
 JOIN custom_field pc ON pc.passenger_id = p.passenger_id
 AND pc.custom_field_name = 'passport_country'
 WHERE p.passenger_id < 5000000;

SELECT last_name,
 first_name,
 coalesce(max(CASE WHEN custom_field_name = 'passport_num'
 THEN custom_field_value ELSE NULL
 END),'') AS passport_num,
 coalesce(max(CASE WHEN custom_field_name = 'passport_exp_date'
 THEN custom_field_value ELSE NULL
 END),'') AS passport_exp_date,
 coalesce(max(CASE WHEN custom_field_name = 'passport_country'
 THEN custom_field_value ELSE NULL
 END),'') AS passport_country
 FROM passenger p
 JOIN custom_field cf USING (passenger_id)
 WHERE cf.passenger_id < 5000000
 AND p.passenger_id < 5000000
 GROUP BY 1,2


select
	last_name,
	first_name,
	passport_num,
	passport_exp_date,
	passport_country
from
	passenger p
join (
	select
		cf.passenger_id,
		coalesce(max(case when custom_field_name = 'passport_num'  then custom_field_value else null end), '') as passport_num,
		coalesce(max(case when custom_field_name = 'passport_exp_date' then custom_field_value else null end), '') as passport_exp_date,
		coalesce(max(case when custom_field_name = 'passport_country' then custom_field_value else null  end), '') as passport_country
	from
		custom_field cf
	where cf.passenger_id < 5 group by 1
 		) info using (passenger_id)
where
	p.passenger_id < 5;


select
		cf.passenger_id,
		coalesce(min(case when custom_field_name = 'passport_num'  then custom_field_value else null end), '') as passport_num,
		coalesce(min(case when custom_field_name = 'passport_exp_date' then custom_field_value else null end), '') as passport_exp_date,
		coalesce(min(case when custom_field_name = 'passport_country' then custom_field_value else null  end), '') as passport_country
	from
		custom_field cf 
	where cf.passenger_id < 5 group by 1;

select
		*
	from
		custom_field cf
	where cf.passenger_id < 5 ;


-- создание таблицы
--
CREATE TABLE boarding_pass_part (
 boarding_pass_id SERIAL,
 passenger_id BIGINT NOT NULL,
 booking_leg_id BIGINT NOT NULL,
 seat TEXT,
 boarding_time TIMESTAMPTZ,
 precheck BOOLEAN NOT NULL,
 update_ts TIMESTAMPTZ
)
PARTITION BY RANGE (boarding_time);
-- создание секций
--
CREATE TABLE boarding_pass_may
PARTITION OF boarding_pass_part
FOR VALUES FROM ('2020-05-01'::timestamptz) TO ('2020-06-01'::timestamptz) ;
--
CREATE TABLE boarding_pass_june
PARTITION OF boarding_pass_part
FOR VALUES FROM ('2020-06-01'::timestamptz) TO ('2020-07-01'::timestamptz);
--
CREATE TABLE boarding_pass_july
PARTITION OF boarding_pass_part
FOR VALUES FROM ('2020-07-01'::timestamptz) TO ('2020-08-01'::timestamptz);
--
CREATE TABLE boarding_pass_aug
PARTITION OF boarding_pass_part
FOR VALUES FROM ('2020-08-01'::timestamptz) TO ('2020-09-01'::timestamptz);
--
INSERT INTO boarding_pass_part SELECT * from boarding_pass;


select * from  boarding_pass_part bpp  where boarding_pass_id = '13328031' and boarding_time ='2020-07-07 08:00:00.000 +0300';

SELECT city,
 date_trunc('month', scheduled_departure),
 sum(passengers) passengers
 FROM airport a
 JOIN flight f ON airport_code = departure_airport
 JOIN (
 SELECT flight_id, count(*) passengers
 FROM booking_leg l
 JOIN boarding_pass b USING (booking_leg_id)
 WHERE boarding_time > '07-15-20'
 AND boarding_time < '07-31-20'
 GROUP BY flight_id
 ) cnt USING (flight_id)
 GROUP BY 1,2
 ORDER BY 3 desc
 
 SELECT city,
 date_trunc('month', scheduled_departure),
 sum(passengers) passengers
 FROM airport a
 JOIN flight f ON airport_code = departure_airport
 JOIN (
 SELECT flight_id, count(*) passengers
 FROM booking_leg l
 JOIN boarding_pass_part b USING (booking_leg_id)
 WHERE boarding_time > '07-15-20'
 AND boarding_time < '07-31-20'
 GROUP BY flight_id
 ) cnt USING (flight_id)
 GROUP BY 1,2
 ORDER BY 3 desc
 
 
 select count(1) from flight
 
 CREATE TABLE flight_no_index AS
 SELECT * FROM flight LIMIT 0;
 
--даже без ключа
INSERT INTO flight_no_index
 SELECT * FROM flight LIMIT 1;

--4 индекса, но в целом есть на всех полях
INSERT INTO flight_all_index 
 SELECT * FROM flight LIMIT 1;
 
-- 3 индекса b-tree
INSERT INTO flight_3_b_tree 
 SELECT * FROM flight LIMIT 1;

-- только pk
INSERT INTO flight_only_ok 
 SELECT * FROM flight LIMIT 1;






