BEGIN
    EXECUTE IMMEDIATE 'drop table fct_prod';
EXCEPTION 
    WHEN others THEN
        NULL;
END;

CREATE SEQUENCE fct_s
    MINVALUE 1
    START WITH 1
    INCREMENT BY 1
    NOCACHE;
	
CREATE TABLE DIM_EVENT_TYPE (
    event_type   VARCHAR2(20), 
    event_id     NUMBER
);

insert into DIM_EVENT_TYPE values ('view', 1);
insert into DIM_EVENT_TYPE values ('cart', 2);
insert into DIM_EVENT_TYPE values ('purchase', 3);
insert into DIM_EVENT_TYPE values ('remove', 4);

CREATE TABLE fct_prod (
    id           NUMBER,
    event_id     NUMBER,
    event_time   DATE, 
    product_id   NUMBER,
    customer_id  NUMBER,
    CONSTRAINT id_pk PRIMARY KEY (id)
);

declare
    start_date  number;
    end_date    number;   
    rdn         number;
    temp        DATE;  
    delta       number;
    l_col_p     number;
    l_col_c     number;
    
begin
    start_date := to_number(to_char(to_date('2020-02-01', 'yyyy-MM-dd'), 'j')); --начало отсчета 
    end_date := to_number(to_char(SYSDATE, 'j')); --конечная дата
    select count(*)+1 
      into l_col_c
      from dim_customers; --id клиента
    select count(*)+1 
      into l_col_p
      from dim_products; --id товара
    
    for cur_r in start_date..end_date 
    loop
        rdn :=dbms_random.value(10, 20); --количество транзакций в течение одного дня
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
end;

--инкрементная загрузка данных
declare
    rdn         number;
    prod_id     number;
    cust_id     number;
    temp        DATE;  
    delta       number;
	l_col_p     number;
    l_col_c     number;
  
begin  
    select SYSDATE into temp from dual; --инициализация текущего времени;
    select max(product_id)+1 into l_col_p from dim_products; --число строк в таблице товаров
	select max(customer_id)+1 into l_col_c from dim_customers;--число строк в таблице клиентов    
    rdn :=dbms_random.value(10,15); --случайное количество транзакций за 5 мин
    delta := 5*60/rdn; --среднее время в сек между транзакциями     
    for i in 1..rdn
    loop
        select (trunc(dbms_random.value(1, l_col_p))) into prod_id from dual; --trunc вместо round для оптимизации запроса
        select (trunc(dbms_random.value(1, l_col_c))) into cust_id from dual;		
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
		temp := temp + numToDSInterval(delta, 'second');  --инкрементация текущего времени;	       
    end loop;      
    commit;
end;