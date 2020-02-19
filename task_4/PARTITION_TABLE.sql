-- если таблица есть, удаляем
BEGIN
    EXECUTE IMMEDIATE 'drop table FCT_EVENTS_part';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Создание таблицы с партицированием по месяцам
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
    -- mounth GENERATED ALWAYS AS (TO_CHAR(event_time, 'mm')) VIRTUAL, -- виртуальный столбец
    CONSTRAINT FCT_EVENTS_part_pk PRIMARY KEY (event_id)
)
PARTITION BY RANGE(event_time)(
    PARTITION MAR values less than (to_date('01.04.2019','DD.MM.YYYY')),
    PARTITION APR values less than (to_date('01.05.2019','DD.MM.YYYY')),
    PARTITION MAY values less than (to_date('01.06.2019','DD.MM.YYYY')),
    PARTITION JUL values less than (to_date('01.07.2019','DD.MM.YYYY')),
    PARTITION AUG values less than (to_date('01.08.2019','DD.MM.YYYY')),
    PARTITION SEP values less than (to_date('01.09.2019','DD.MM.YYYY')),
    PARTITION OCT values less than (to_date('01.10.2019','DD.MM.YYYY')),
    PARTITION JUN values less than (to_date('01.11.2019','DD.MM.YYYY')),
    PARTITION NOV values less than (to_date('01.12.2019','DD.MM.YYYY')),
    PARTITION DEC values less than (to_date('01.01.2020','DD.MM.YYYY')),
    PARTITION JAN values less than (to_date('01.02.2020','DD.MM.YYYY')),
    PARTITION FEB values less than (maxvalue)
);

-- Для исследования влияния времени лучше использовать разбиение по хеш функции.
-- У нас не будет данных за весь год, а при разбиении по месяцам это приведет конкретной
-- неравномерному разбиению.
-- Разбиение по хешу будет более равномерно. Так будет лучше видно влияние на время обработки запроса.
/*
CREATE TABLE FCT_EVENTS_part(
    event_time DATE,
    event_type VARCHAR2(20),
    event_id NUMBER,
    product_id NUMBER,
    category_id NUMBER,
    category_code VARCHAR2(25),
    brand VARCHAR2(30),
    price NUMBER,
    customer_id NUMBER,
    mounth GENERATED ALWAYS AS (TO_CHAR(event_time, 'mm')) VIRTUAL, -- виртуальный столбец
    CONSTRAINT FCT_EVENTS_part_pk PRIMARY KEY (event_id)
)
PARTITION BY HASH(event_time) PARTITIONS 12;
*/

-- Вставка данных из обычной таблицы
INSERT INTO FCT_EVENTS_part (event_time, event_type, event_id, product_id, category_id, category_code, brand, price, customer_id)
SELECT * FROM FCT_EVENTS


-----------------------------------------------
--------------- Доп. информация ---------------
-----------------------------------------------

-- Выбор данных из конкретной партиции
-- SELECT * FROM FCT_EVENTS_part PARTITION(NOV)

-- Просмотр типа столбцов в таблице
-- SELECT
--     OWNER,
--     TABLE_NAME,
--     COLUMN_NAME,
--     VIRTUAL_COLUMN,
--     DATA_DEFAULT
-- FROM ALL_TAB_COLS
-- WHERE TABLE_NAME = 'FCT_EVENTS_PART';

-- Просмотр допустимых tablespace
-- SELECT TABLESPACE_NAME FROM dba_tablespaces;

-- Запрос для проверки, что партиции правильно создались
-- SELECT
--     table_name,
--     partition_name,
--     high_value,
--     tablespace_name
-- FROM user_tab_partitions
-- WHERE table_name = 'FCT_EVENTS_PART'
