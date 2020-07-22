-- Таблица Логов --

BEGIN
    EXECUTE IMMEDIATE 'drop table log_table';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Последовательность для id таблицы Фактов --

BEGIN
    EXECUTE IMMEDIATE 'drop SEQUENCE fct_s_prod';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Подсправочник имен --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_NAMES';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Подсправочник  фамилий --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_LASTNAMES';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


-- Справочник Покупатели --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_CUSTOMERS';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


-- Подсправочник  Продуктов --

BEGIN
    EXECUTE IMMEDIATE 'drop table products';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Подсправочник  Брендов --

BEGIN
    EXECUTE IMMEDIATE 'drop table brands';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Справочник Категорий --

BEGIN
    EXECUTE IMMEDIATE 'drop table CATEGORY';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


-- Справочник Товары --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_PRODUCTS';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Последовательность для id таблицы Поставщики --

BEGIN
    EXECUTE IMMEDIATE 'drop SEQUENCE suppliers_s';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Справочник Поставщики --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_SUPPLIERS';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Справочник События --

BEGIN
    EXECUTE IMMEDIATE 'drop table DIM_EVENT_TYPE';
EXCEPTION 
    WHEN others THEN
        NULL;
END;


-- Таблица Фактов --

BEGIN
    EXECUTE IMMEDIATE 'drop table FCT_PROD';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Спецификация пакета --

BEGIN
    EXECUTE IMMEDIATE 'drop package okko';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

-- Тело пакета --

BEGIN
    EXECUTE IMMEDIATE 'drop package body okko';
EXCEPTION 
    WHEN others THEN
        NULL;
END;
