--генерируемая короткая таблица событий
create table fct_short (
event_time DATE, 
event_type VARCHAR2(100), 
event_id NUMBER, 
product_id NUMBER,
customer_id NUMBER
);

-- заполнение историей короткой таблицы фактов
declare
  start_date number;
  end_date number;   
  rdn number;
  prod_id number;
  cust_id number;
  ev_type varchar2(20);
  ev_id number;
  temp timestamp;  
  delta number;
  
begin
  start_date := to_number(to_char(to_date('2020-02-10', 'yyyy-MM-dd'), 'j')); --начало отсчета 
  end_date := to_number(to_char(current_timestamp, 'j')); --конечная дата
  
  for cur_r in start_date..end_date 
  loop
      rdn :=dbms_random.value(1000,5000); --количество транзакций в течение одного дня
      delta := 24*60*60/rdn; --среднее время между транзакциями
      select (cast(to_date(cur_r, 'j') as timestamp)) into temp from dual; --инициализация текущего дня
      for i in 1..rdn
      loop
          select (round(dbms_random.value(1, (select count(*) from dim_products)))) into prod_id from dual; --id товара
          select (round(dbms_random.value(1, (select count(*) from dim_customers)))) into cust_id from dual; --id клиента
          select count(*)+1 into ev_id from fct_short; --id события 
          select decode(round(dbms_random.value(1,9)), 1, 'view', 2, 'view', 3, 'view', 4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 
                                        8, 'remove', 9, 'purchase') into ev_type from dual; --вероятность событий
          select (temp+numToDSInterval(delta, 'second')) into temp from dual; --инкрементация текущей даты

          insert into fct_short (event_time, event_id, event_type, product_id, customer_id) values
          (temp, ev_id, ev_type, prod_id, cust_id);
      end loop;           
  end loop;
commit;
end;


-- таблица фактов
create table fct_events (event_time, event_type, event_id primary key, product_id, category_code, brand, price, customer_id) as 
(select cast(sh.event_time as timestamp) as event_time, sh.event_type, sh.event_id, sh.product_id prod,
       pr.category_code, pr.brand, pr.price,
       sh.customer_id
       from fct_short sh
       join dim_products pr on sh.product_id = pr.product_id)
	   
-- добавление новых данных в общую таблицу фактов
declare
  start_date number;
  end_date number;   
  rdn number;
  prod_id number;
  cust_id number;
  ev_type varchar2(20);
  ev_id number;
  temp timestamp;  
  delta number;
  cc VARCHAR2(25);
  br VARCHAR2(25);
  prc NUMBER;
  
begin  
      rdn :=dbms_random.value(400,800); --количество транзакций в течение одного дня
      delta := 5*60/rdn; --среднее время между транзакциями за 5 мин
      select current_timestamp into temp from dual; --инициализация текущего времени;
      for i in 1..rdn
      loop
          select (round(dbms_random.value(1, (select count(*) from dim_products)))) into prod_id from dual;
          select (round(dbms_random.value(1, (select count(*) from dim_customers)))) into cust_id from dual;
          select count(*)+1 into ev_id from fct_EVENTS;
          select decode(round(dbms_random.value(1,9)), 1, 'view', 2, 'view', 3, 'view', 4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 
                                        8, 'remove', 9, 'purchase') into ev_type from dual; --вероятность событий
          select (temp+numToDSInterval(delta, 'second')) into temp from dual; --инкрементация времени          
          select category_code into cc from dim_products dim_p where dim_p.product_id = prod_id; --category code из dim_products
          select brand into br from dim_products dim_p where dim_p.product_id = prod_id;--brand из dim_products
          select price into prc from dim_products dim_p where dim_p.product_id = prod_id;--price из dim_products
          insert into fct_events (event_time, event_id, event_type, product_id, customer_id, category_code, brand, price) values
          (temp, ev_id, ev_type, prod_id, cust_id, cc, br, prc);          
      end loop;      
commit;
end;