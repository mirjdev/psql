### Заметки по книге: Оптимизация запросов PostgreSQL - 2022 Домбровская Г., Новиков Б., Бейликова А.

#### Приведены примеры запросов, планы запросов, скрины - air.sql

- Создание индексов полнотекстового поиска GIN / GiST
- Примеры запросов полнотекстового поиска
- Генерируемые столбцы GENERATED ALWAYS AS ... STORED
- Создание индексов для jsonb/json GIN/B-TREE
- Примеры запросов поиска в json
- Создание индексов B-TREE с поиском lower / like 'name%'
- Замеры производительности при вставке записей, при различном числе индексов 
- Создание таблицы с партициями (по месяцам)

#### other.sql примеры, лучшие практики, не по книге
- представления
- настройки 
- индексы, триграммы like '%поиск по такому лайку%'
- рекурсия
- оконные функции
- возвращаемые значения

#### sets.sql операции над множествами 


### Полезные ссылки 
- https://habr.com/ru/companies/postgrespro/articles/462877/ -- Егор Рогов postgrespro
- https://habr.com/ru/users/Kilor/ -- Тензор
- https://github.com/dataegret/pg-utils/tree/master/sql - полезные скрипты от Data Egret