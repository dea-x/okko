-------------------------------------------------------
-------------------- Таблица логов --------------------
-------------------------------------------------------

-- Удаление таблицы, если она уже есть
BEGIN
    EXECUTE IMMEDIATE 'drop table log_table';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


CREATE TABLE log_table (
    time_log            TIMESTAMP,
    level_log           VARCHAR2(10),
    program_name        VARCHAR2(40),
    procedure_name      VARCHAR2(40),
    message             VARCHAR2(1000)
);


/*
CREATE TABLE log_error (
    PROGRAM_NAME        VARCHAR2(10),
    procedure_name      VARCHAR2(20),
    code                VARCHAR2(20),
    error_message       VARCHAR2(1000),
    error_position      VARCHAR2(500),
    error_time          TIMESTAMP
);
*/



