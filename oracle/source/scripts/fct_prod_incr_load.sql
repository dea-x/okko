BEGIN
  --DBMS_SCHEDULER.DROP_JOB ('SYS.OKKO_JOB', force => true);
  DBMS_SCHEDULER.CREATE_JOB(
  JOB_NAME => 'okko_job',
  JOB_TYPE => 'PLSQL_BLOCK',
  JOB_ACTION => 'BEGIN test_user.okko.FILL_FCT_PROD_DISCRETE(2);  END;',
  START_DATE => SYSTIMESTAMP,
  REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=30',
  END_DATE => SYSTIMESTAMP + INTERVAL '17' HOUR,
  COMMENTS => 'Insert new data into source DB',
  ENABLED => TRUE);
END;


CREATE OR REPLACE PACKAGE OKKO IS
    PROCEDURE FILL_CUSTOMERS (c_id_end IN NUMBER);
    PROCEDURE FILL_PRODUCTS (bins IN NUMBER);
    PROCEDURE FILL_FCT_PROD;
    PROCEDURE FILL_FCT_PROD_DISCRETE (days IN NUMBER);
END OKKO;


-- Создание тела пакета
CREATE OR REPLACE PACKAGE BODY OKKO IS
    PROGRAM_NAME CONSTANT VARCHAR2(5) := 'OKKO';
    ------------------------------------------------------------------------------
    ------------------------- Процедура для записи логов -------------------------
    ------------------------------------------------------------------------------
    PROCEDURE write_log (level_log IN VARCHAR2, program_name IN VARCHAR2, procedure_name IN VARCHAR2,
                         message IN VARCHAR2) 
    IS
        -- включение автономных транзакций
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        insert into log_table values (systimestamp, level_log, program_name, procedure_name, message);
        commit;
    END write_log;


    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_SUPPLIERS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_SUPPLIERS IS
         -- создание массива для хранения городов
        type arr_type_city is table of VARCHAR2(40)
        index by binary_integer;
        category_id          NUMBER;
        name                 VARCHAR2(40); 
        country              VARCHAR2(40); 
        city                 VARCHAR2(40);
        cities               arr_type_city;
        city_index           NUMBER;
        flag                 NUMBER(1);
        procedure_name       VARCHAR2(100) := 'FILL_SUPPLIERS';
        message              VARCHAR2(1000);
        i                    NUMBER;
    BEGIN
        cities(1) := 'Москва';
        cities(2) := 'Санкт-Петербург';
        cities(3) := 'Волгоград';
        cities(4) := 'Калининград';
        cities(5) := 'Астрахань';
        cities(6) := 'Тула';
        country := 'Россия';
        -- получение всех доступных кодов категорий
        i := 1;
        for rowCategory in (select category_id from category) loop
            -- получаем 1, если этот код уже есть в поставщиках, иначе 0
            select case when exists(select * from DIM_SUPPLIERS 
                                    where category_id = rowCategory.category_id)
                        then 1 
                        else 0
                    end
                into flag
                from dual;
            -- если кода нет, добавляем в таблицу
            if flag = 0 then
                city_index := mod(round(DBMS_RANDOM.VALUE * 100), cities.count) + 1;
                city := cities(city_index);
                name := 'OOO '||DBMS_RANDOM.STRING('a', 4);
                insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, rowCategory.category_id, 
                                                  name, country, city, sysdate);
            end if;
            i := i + 1;
        end loop;
        commit;
        if i > 0 then
            message := 'Successful writing of '||TO_CHAR(i)||' lines';
            write_log('INFO', PROGRAM_NAME, procedure_name, message);
        end if;
    EXCEPTION WHEN others THEN
        message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
        write_log('ERROR', PROGRAM_NAME, procedure_name, message);
    END FILL_SUPPLIERS;


    -----------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_PRODUCTS -----------------
    -----------------------------------------------------------------------------
    PROCEDURE FILL_PRODUCTS (bins IN NUMBER) IS
        procedure_name       VARCHAR2(100) := 'FILL_PRODUCTS';
        message              VARCHAR2(1000);
    BEGIN
        for i in 1..bins loop 
            insert into dim_products (description, name, product_id, category_id, brand, price, last_update_date)
                (select ft.des, ft.product, -- description, name
                (select count(*) + 1 from dim_products), -- product_id
                ft.category_id, -- category_id
                (select brand from -- brand
                (select brand, dbms_random.value() rnd from brands order by rnd) fetch first 1 rows only),
                round(dbms_random.value(2000, 10000)),
                SYSDATE
            from 
                (select category_id, product, des, dbms_random.value(1, (select count(*) from products)) rd 
                from products order by rd) ft fetch first 1 rows only
            );
        end loop;
        commit;
        message := 'Successful writing of '||TO_CHAR(bins)||' lines';
        write_log('INFO', PROGRAM_NAME, procedure_name, message);
        FILL_SUPPLIERS;
    EXCEPTION WHEN others THEN
        message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
        write_log('ERROR', PROGRAM_NAME, procedure_name, message);
    end FILL_PRODUCTS;


    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_CUSTOMERS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_CUSTOMERS (c_id_end IN NUMBER) IS
        c_id_st              NUMBER;
        step                 NUMBER;
        c_country            VARCHAR2(40); 
        c_city               VARCHAR2(40);  
        c_phone              VARCHAR2(40); 
        c_fname              VARCHAR2(40); 
        c_lname              VARCHAR2(40); 
        c_mail               VARCHAR2(50);
        c_last_update_date   DATE;
        procedure_name       VARCHAR2(100) := 'FILL_CUSTOMERS';
        message              VARCHAR2(1000);
    BEGIN
        -- выбираем максимальный итерируемый id    
        select max(customer_id) into c_id_st from dim_customers;
        -- проверяем не пустая ли таблица, что бы не словить null в итератор
        if c_id_st IS NULL then
            c_id_st:=1; 
        end if;
        c_country := 'Россия';
        -- выставляем количество шагов итерации
        step := c_id_end + c_id_st;
        while c_id_st < step loop
            c_id_st := c_id_st + 1;
            select decode(abs(mod(DBMS_RANDOM.RANDOM, 5)), 0, 'Мосвка', 1, 'Санкт-Петербург', 2, 'Воронеж', 3, 'Мурманск', 4, 'Волгоград') into c_city from dual;
            select '8-'||decode(abs(mod(DBMS_RANDOM.RANDOM, 3)), 0, '903', 1, '909', '916')||'-'||to_char(mod(abs(DBMS_RANDOM.RANDOM), 8000000)+1000000) into c_phone from dual;
                if mod(mod(c_id_st, 0), 10) = 0 THEN
                    select name into c_fname from (select name, dbms_random.value() rnd from dim_names where gender = 'ж' order by rnd) fetch first 1 rows only;
                    select name into c_lname from (select name, dbms_random.value() rnd from dim_lastnames where gender = 'ж' order by rnd) fetch first 1 rows only;
                else 
                    select name into c_fname from (select name, dbms_random.value() rnd from dim_names where gender = 'м' order by rnd) fetch first 1 rows only;
                    select name into c_lname from (select name, dbms_random.value() rnd from dim_lastnames where gender = 'м' order by rnd) fetch first 1 rows only;
                end if;
            select DBMS_RANDOM.STRING('U',1)||DBMS_RANDOM.STRING('L', DBMS_RANDOM.VALUE(4,8))||decode(abs(mod(DBMS_RANDOM.RANDOM, 3)), 0, '@gmail.com', 1, '@mail.ru', 2, '@ya.ru') into c_mail from dual;
            select SYSDATE into c_last_update_date from dual;
            -- вставляем сгенерированные данные в конечный справочник DIM_CUSTOMERS
            insert into dim_customers values (c_id_st, c_country, c_city, c_phone, c_fname, c_lname, c_mail, c_last_update_date);
            c_id_st := c_id_st + 1;
        end loop;
        commit;
        message := 'Successful writing of '||TO_CHAR(c_id_end)||' lines';
        write_log('INFO', PROGRAM_NAME, procedure_name, message);
    EXCEPTION WHEN others THEN
        message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
        write_log('ERROR', PROGRAM_NAME, procedure_name, message);
    end FILL_CUSTOMERS;
    
    
    -----------------------------------------------------------------------------
    ------------------ Процедура наполнения таблицы FCT_EVENTS ------------------
    -----------------------------------------------------------------------------
     PROCEDURE FILL_FCT_PROD IS
        rdn                  NUMBER;
        -- procedure_name       VARCHAR2(100) := 'FILL_FCT_PROD';
        -- message              VARCHAR2(1000);
    BEGIN  
        rdn := dbms_random.value(5100, 10200); --случайное количество транзакций за 5 мин
        FILL_FCT_PROD_DISCRETE(rdn);
    -- EXCEPTION WHEN others THEN
        -- message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
        -- write_log('ERROR', PROGRAM_NAME, procedure_name, message);
    END FILL_FCT_PROD;
    
    
    ----------------------------------------------------------------------------------------
    ---------- Процедура наполнения таблицы FCT_EVENTS на указанное число записей ----------
    ----------------------------------------------------------------------------------------
    PROCEDURE FILL_FCT_PROD_DISCRETE (days IN NUMBER) IS   		
    start_date  date;
    end_date    date;   
    rdn         number;
    temp        DATE;  
    delta       number;
    l_col_p     number;
    l_col_c     number;
	procedure_name       VARCHAR2(100) := 'FILL_FCT_PROD_DISCRETE';
    message              VARCHAR2(1000);
    
    begin
        select trunc(max(event_time))+1 into start_date from fct_prod;
        end_date := start_date+days;
        select count(*)+1 
          into l_col_c
          from dim_customers; --id клиента
        select count(*)+1 
          into l_col_p
          from dim_products; --id товара
        
        for cur_r in to_number(to_char(start_date, 'j'))..to_number(to_char(end_date, 'j')) 
        loop
            rdn :=dbms_random.value(1000000, 1100000); --количество транзакций в течение одного дня
            delta := 24*60*60/rdn; --среднее время между транзакциями
            select (to_date(cur_r, 'j')) into temp from dual; --инициализация текущего дня
            for i in 1..rdn
            loop             
                insert into fct_prod (id, event_id, event_time, product_id, customer_id) values
                (fct_s.NEXTVAL,  --id события 
                decode(trunc(dbms_random.value(1,10)), 1, 1, 2, 1, 3, 1, 4, 1,
                                                       5, 2, 6, 2, 7, 2,
                                                       8, 3,
                                                       9, 4), --вероятность событий
                temp,           
                trunc(dbms_random.value(1, l_col_p)), -- product_id
                trunc(dbms_random.value(1, l_col_c))  -- product_id
                );
            temp := temp + numToDSInterval(delta, 'second');  --инкрементация текущей даты 
        end loop;
        commit;
        end loop;        
        message := 'Successful writing of '||TO_CHAR(rdn)||' lines';
        write_log('INFO', PROGRAM_NAME, procedure_name, message);
    EXCEPTION WHEN others THEN
        message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
        write_log('ERROR', PROGRAM_NAME, procedure_name, message);
    END FILL_FCT_PROD_DISCRETE;

END OKKO;