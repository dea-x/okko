--генерируемая короткая таблица событий
create table fct_short (
event_time DATE, 
event_type VARCHAR2(100), 
event_id NUMBER, 
product_id NUMBER,
customer_id NUMBER
);

declare
rdn number;
prod_id number;
cust_id number;
tmst date;
ev_type varchar2(20);
ev_id number;

begin
rdn :=dbms_random.value(1000,5000);

for i in 1..rdn
loop
select (round(dbms_random.value(1, (select count(*) from dim_products)))) into prod_id from dual;
select (round(dbms_random.value(1, (select count(*) from dim_customers)))) into cust_id from dual;
select count(*) into ev_id from fct_short;
select decode(round(dbms_random.value(1,9)), 1, 'view', 2, 'view', 3, 'view', 4, 'view', 5, 'cart', 6, 'cart', 7, 'cart', 
                                        8, 'remove', 9, 'purchase') into ev_type from dual;
select current_timestamp into tmst from dual;

insert into fct_short (event_time, event_id, event_type, product_id, customer_id) values
(tmst, ev_id, ev_type, prod_id, cust_id);

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