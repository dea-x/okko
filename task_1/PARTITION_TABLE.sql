-- если таблица есть, удаляем
BEGIN
    EXECUTE IMMEDIATE 'drop table FCT_EVENTS_part';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Создание таблицы с партицированием по месяцам
CREATE TABLE FCT_EVENTS_part(
    event_time TIMESTAMP,
    event_type VARCHAR2(20),
    event_id NUMBER,
    product_id NUMBER,
    category_id NUMBER,
    category_code VARCHAR2(25),
    brand VARCHAR2(30),
    price NUMBER,
    customer_id NUMBER,
    mounth GENERATED ALWAYS AS (TO_CHAR(event_time, 'mm')) VIRTUAL, -- виртуальный столбец
    CONSTRAINT event_pk PRIMARY KEY (event_id)
)
PARTITION BY LIST(mounth) (
    PARTITION JAN VALUES ('01'),
    PARTITION FEB VALUES ('02'),
    PARTITION MAR VALUES ('03'),
    PARTITION APR VALUES ('04'),
    PARTITION MAY VALUES ('05'),
    PARTITION JUN VALUES ('06'),
    PARTITION JUL VALUES ('07'),
    PARTITION AUG VALUES ('08'),
    PARTITION SEP VALUES ('09'),
    PARTITION OCT VALUES ('10'),
    PARTITION NOV VALUES ('11'),
    PARTITION DEC VALUES ('12')
);

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
