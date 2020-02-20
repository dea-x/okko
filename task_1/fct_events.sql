-- Удаление таблицы, если она уже есть

BEGIN
    EXECUTE IMMEDIATE 'drop table fct_short';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

CREATE SEQUENCE fct_s
    MINVALUE 1
    START WITH 1
    INCREMENT BY 1
    NOCACHE;

CREATE TABLE fct_short (
    event_time   DATE, 
    event_type   VARCHAR2(20), 
    event_id     NUMBER, 
    product_id   NUMBER,
    customer_id  NUMBER
);


declare
    start_date  number;
    end_date    number;   
    rdn         number;
    prod_id     number;
    cust_id     number;
    ev_type     varchar2(20);
    ev_id       number;
    temp        DATE;  
    delta       number;
    l_col_p     number;
    l_col_c     number;
    
begin
    start_date := to_number(to_char(to_date('2019-03-01', 'yyyy-MM-dd'), 'j')); --начало отсчета 
    end_date := to_number(to_char(SYSDATE, 'j')); --конечная дата
    select count(*) 
      into l_col_c
      from dim_customers; --id клиента
    select count(*) 
      into l_col_p
      from dim_products; --id товара
    
    for cur_r in start_date..end_date 
    loop
        rdn :=dbms_random.value(1000, 5000); --количество транзакций в течение одного дня
        delta := 24*60*60/rdn; --среднее время между транзакциями
        select (to_date(cur_r, 'j')) into temp from dual; --инициализация текущего дня
        for i in 1..rdn
        loop
            temp := temp + numToDSInterval(delta, 'second');  --инкрементация текущей даты  
            insert into fct_short (event_time, event_id, event_type, product_id, customer_id) values
                (temp, 
                fct_s.NEXTVAL,  --id события 
                decode(round(dbms_random.value(1,9)), 1, 'view', 2, 'view', 3, 'view', --вероятность событий
                                                4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 8, 'remove', 9, 'purchase'), 
                round(dbms_random.value(1, l_col_p)), 
                round(dbms_random.value(1, l_col_c)));
        end loop;
        commit;
    end loop;
    commit;
end;


-- таблица фактов
CREATE TABLE FCT_EVENTS (
    event_time     DATE,
    event_type     VARCHAR2(20),
    event_id       NUMBER, 
	product_id     NUMBER,
    category_id    NUMBER,
    category_code  VARCHAR2(25),
    brand          VARCHAR2(30), price NUMBER,
    customer_id    NUMBER,
    CONSTRAINT event_pk PRIMARY KEY (event_id)
);

INSERT INTO FCT_EVENTS (select sh.event_time as event_time, sh.event_type, sh.event_id, sh.product_id, pr.category_id,
    pr.category_code, pr.brand, pr.price,
    sh.customer_id
    from fct_short sh
    join dim_products pr on sh.product_id = pr.product_id)
	   
-- добавление новых данных в общую таблицу фактов
declare
    rdn         number;
    prod_id     number;
    cat_id      number;
    cust_id     number;
    ev_type     varchar2(20);
    ev_id       number;
    temp        DATE;  
    delta       number;
    cc          VARCHAR2(25);
    br          VARCHAR2(30);
    prc         NUMBER;
  
begin  
    rdn :=dbms_random.value(17,34); --количество транзакций в сек
    delta := 5*60/rdn; --среднее время в сек между транзакциями
    select SYSDATE into temp from dual; --инициализация текущего времени;
    for i in 1..rdn
    loop
        select (round(dbms_random.value(1, (select count(*) from dim_products)))) into prod_id from dual;
        select (round(dbms_random.value(1, (select count(*) from dim_customers)))) into cust_id from dual;
        select count(*)+1 into ev_id from fct_EVENTS;
        select decode(round(dbms_random.value(1,9)), 1, 'view', 2, 'view', 3, 'view', 4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 
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
end;