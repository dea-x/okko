/* Пакет для заполнения данных таблицы фактов и таблиц справочников
   для проекта OKKO.
   
   Вызов процедур пакета:
   BEGIN
       okko.FILL_customers (1);  -- добавление в таблицу покупателей 1 записи
       okko.FILL_products (1);  -- добавление в таблицу продуктов 1 записи
       okko.FILL_fct_events;  -- добавление записей в таблицу фактов
   END;
*/


-- Объявление пакета
CREATE OR REPLACE PACKAGE OKKO IS
    PROCEDURE FILL_customers (c_id_end IN NUMBER);
    PROCEDURE FILL_products (bins IN NUMBER);
    PROCEDURE FILL_fct_events;
END OKKO;

-- Создание тела пакета
CREATE OR REPLACE PACKAGE BODY OKKO IS
    PROGRAM_NAME CONSTANT VARCHAR2(5) := 'OKKO';
    ------------------------------------------------------------------------------
    ------------------------- Процедура для записи логов -------------------------
    ------------------------------------------------------------------------------
    PROCEDURE log_err (PROGRAM_NAME IN VARCHAR2, procedure_name IN VARCHAR2, code IN VARCHAR2, 
                       error_message IN VARCHAR2, error_position IN VARCHAR2) 
    IS
        -- включение автономных транзакций
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        insert into log_error values (PROGRAM_NAME, procedure_name, code, 
                                      error_message, error_position, systimestamp);
        commit;
    END log_err;


    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_SUPPLIERS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_SUPPLIERS IS
         -- создание массива для хранения городов
        type arr_type_city is table of VARCHAR2(40)
        index by binary_integer;
        category             VARCHAR2(25);
        name                 VARCHAR2(40); 
        country              VARCHAR2(40); 
        city                 VARCHAR2(40);
        cities               arr_type_city;
        city_index           NUMBER;
        flag                 NUMBER(1);
        procedure_name       VARCHAR2(1000) := 'FILL_SUPPLIERS';
        
    BEGIN
        cities(1) := 'Москва';
        cities(2) := 'Санкт-Петербург';
        cities(3) := 'Волгоград';
        cities(4) := 'Калининград';
        cities(5) := 'Астрахань';
        cities(6) := 'Тула';
        country := 'Россия';
        -- получение всех доступных кодов категорий
        for rowCategory in (select distinct category_code from dim_products) loop
            -- получаем 1, если этот код уже есть в поставщиках, иначе 0
            select case when exists(select * from DIM_SUPPLIERS 
                                where category = rowCategory.category_code)
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
            
                insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, rowCategory.category_code, 
                                                  name, country, city, sysdate);
            end if;
        end loop;
        commit;
    EXCEPTION WHEN others THEN
        log_err(PROGRAM_NAME, procedure_name, TO_CHAR(sqlcode), sqlerrm, dbms_utility.format_error_backtrace);
    END FILL_SUPPLIERS;


    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_PRODUCTS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_PRODUCTS (bins IN NUMBER) IS
        -- bins number;
        r                    NUMBER;
        procedure_name       VARCHAR2(1000) := 'FILL_PRODUCTS';
    begin
        -- при создании задаем большое число товаров; если таблица существует, объявляем инкремент
        -- select count(*)+1 into r from dim_products;
        -- if (r < 11) then
        --     bins := 10;
        -- else 
        --     bins := 2;
        -- end if;
        for i in 1..bins loop 
            insert into dim_products (category_code, description, name, product_id, category_id, brand, price, last_update_date)
                (select ft.category_code, ft.des, ft.product,
                    (select count(*)+1 from dim_products),
                    (select decode(category_code, 'Дом', 1, 'Кухня', 2, 'Красота', 3, 'Mobile', 4) from dual), 
                    (select brand from 
                        (select brand, dbms_random.value() rnd from brands order by rnd) fetch first 1 rows only),
                    round(dbms_random.value(2000, 10000)),
                    SYSDATE
                from 
                    (select category_code, product, des, dbms_random.value(1, (select count(*) from products)) rd 
                    from products order by rd) ft fetch first 1 rows only);
        end loop;
        commit;
        FILL_SUPPLIERS;
    EXCEPTION WHEN others THEN
        log_err(PROGRAM_NAME, procedure_name, TO_CHAR(sqlcode), sqlerrm, dbms_utility.format_error_backtrace);
    end FILL_PRODUCTS;


    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы DIM_CUSTOMERS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_CUSTOMERS (c_id_end IN NUMBER) IS
        c_id_st              NUMBER;
        c_country            VARCHAR2(40); 
        c_city               VARCHAR2(40);  
        c_phone              VARCHAR2(40); 
        c_fname              VARCHAR2(40); 
        c_lname              VARCHAR2(40); 
        c_mail               VARCHAR2(50);
        c_last_update_date   DATE;
        procedure_name       VARCHAR2(1000) := 'FILL_CUSTOMERS';
    begin
        -- выбираем максимальный итерируемый id    
        select max(customer_id) into c_id_st from dim_customers;
        -- проверяем не пустая ли таблица, что бы не словить null в итератор
        if c_id_st IS NULL then
            c_id_st:=1; 
        end if;
        c_country := 'Россия';
        -- выставляем количество шагов итерации
        -- c_id_end := c_id_st + 20;
        -- выполяем итерацию пока
        while c_id_st <= c_id_end loop
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
    EXCEPTION WHEN others THEN
        log_err(PROGRAM_NAME, procedure_name, TO_CHAR(sqlcode), sqlerrm, dbms_utility.format_error_backtrace);
    end FILL_CUSTOMERS;
    
    
    ------------------------------------------------------------------------------
    ----------------- Процедура наполнения таблицы FCT_EVENTS -----------------
    ------------------------------------------------------------------------------
    PROCEDURE FILL_fct_events IS
        start_date           NUMBER;
        end_date             NUMBER;   
        rdn                  NUMBER;
        prod_id              NUMBER;
        cat_id               NUMBER;
        cust_id              NUMBER;
        ev_type              VARCHAR2(20);
        ev_id                NUMBER;
        temp                 DATE;  
        delta                NUMBER;
        cc                   VARCHAR2(25);
        br                   VARCHAR2(25);
        prc                  NUMBER;
        procedure_name       VARCHAR2(1000) := 'FILL_FCT_EVENTS';
    begin  
        rdn := dbms_random.value(5100, 10200); --количество транзакций в сек
        delta := 5 * 60 / rdn; --среднее время в сек между транзакциями
        select SYSDATE into temp from dual; --инициализация текущего времени;
        for i in 1..rdn
        loop
            select (round(dbms_random.value(1, (select count(*) from dim_products)))) into prod_id from dual;
            select (round(dbms_random.value(1, (select count(*) from dim_customers)))) into cust_id from dual;
            select count(*) + 1 into ev_id from fct_EVENTS;
            select decode(round(dbms_random.value(1, 9)), 1, 'view', 2, 'view', 3, 'view', 4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 
                                                          8, 'remove', 9, 'purchase') into ev_type from dual; --вероятность событий                                        
            select (temp+numToDSInterval(delta, 'second')) into temp from dual; --инкрементация времени          
            select category_code into cc from dim_products dim_p where dim_p.product_id = prod_id; --category code из dim_products
            select category_id into cat_id from dim_products dim_p where dim_p.product_id = prod_id;
            select brand into br from dim_products dim_p where dim_p.product_id = prod_id;--brand из dim_products
            select price into prc from dim_products dim_p where dim_p.product_id = prod_id;--price из dim_products
            insert into fct_events (event_time, event_id, event_type, product_id, category_id, customer_id, category_code, brand, price) values
                                   (temp, ev_id, ev_type, prod_id, cat_id, cust_id, cc, br, prc);          
        end loop;      
        commit;
    EXCEPTION WHEN others THEN
        log_err(PROGRAM_NAME, procedure_name, TO_CHAR(sqlcode), sqlerrm, dbms_utility.format_error_backtrace);
    end FILL_fct_events;

END OKKO;
