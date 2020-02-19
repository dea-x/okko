-------------------------------------------------------
-------------------- Таблица логов --------------------
-------------------------------------------------------

-- Удаление таблицы, если она уже есть
BEGIN
    EXECUTE IMMEDIATE 'drop table log_error';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


CREATE TABLE log_error (
    PROGRAM_NAME        VARCHAR2(10),
    procedure_name      VARCHAR2(20),
    code                VARCHAR2(20),
    error_message       VARCHAR2(1000),
    error_position      VARCHAR2(500),
    error_time          TIMESTAMP
);
