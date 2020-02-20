-------------------------------------------------
-------------------- Индексы --------------------
-------------------------------------------------
/*
Таблица, которую будем индексировать:

CREATE TABLE FCT_EVENTS_part(
    event_time         DATE,
    event_type         VARCHAR2(20),
    event_id           NUMBER,
    product_id         NUMBER,
    category_id        NUMBER,
    category_code      VARCHAR2(25),
    brand              VARCHAR2(30),
    price              NUMBER,
    customer_id        NUMBER,
    CONSTRAINT FCT_EVENTS_part_pk PRIMARY KEY (event_id)
);

В таблице присутсвует первичный ключ, то есть таблице сразу проиндексирована по event_id.
Также таблица разбина на партиции по event_type по месяцу, то есть на этот столбец индекс не нужен.
Можем добавить BITMAP индексы на столбцы event_type, category_code или brand. Но надо
узнать, какие запросы будут в будущем по таблице. Если будет проводится выбор по какому то 
event_type, то делаем BITMAP на event_type и также с сотальными.
*/

-- Слово local требутется, так как таблица разбита на партиции.
-- В каждой партиции создается свой BITMAP индекс
-- PARALLEL значит, что сканирование по индексу может идти паралельно по партициям
-- NOLOGGING для уменьшения записей в redo логи.

-- Создание BITMAP индексов на столбец event_type
create BITMAP index event_type_idx on FCT_EVENTS_part(event_type) local PARALLEL NOLOGGING;
-- Создание BITMAP индексов на столбец category_code
create BITMAP index category_code_idx on FCT_EVENTS_part(category_code) local PARALLEL NOLOGGING;
-- Создание BITMAP индексов на столбец brand
create BITMAP index brand_idx on FCT_EVENTS_part(brand) local PARALLEL NOLOGGING;
