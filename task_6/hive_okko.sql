CREATE EXTERNAL TABLE dim_customers (
    customer_id INT,
    country STRING, 
    city STRING, 
    phone STRING, 
    first_name STRING, 
    last_name STRING, 
    email STRING,
    last_update_date STRING, 
    primary key(customer_id) disable novalidate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/MAI/dim_customers/';


CREATE EXTERNAL TABLE dim_products (
    product_id INT, 
    category_id INT, 
    brand STRING, 
    description STRING, 
    name STRING, 
    price INT, 
    last_update_date STRING, 
    primary key(product_id) disable novalidate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/MAI/dim_products/';


CREATE EXTERNAL TABLE dim_suppliers (
    suppliers_id INT, 
    category_id INT, 
    name STRING, 
    country STRING, 
    city STRING, last_update_date DATE, 
    primary key(suppliers_id) disable novalidate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/MAI/dim_suppliers/';


CREATE EXTERNAL TABLE fct_prod (
    id INT, 
    event_id INT, 
    IMSI STRING, 
    event_time STRING, 
    product_id INT, 
    customer_id INT, 
primary key(id) disable novalidate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/MAI/fct_prod/';

select cast(substr(last_update_date,1,10) as date) from dim_customers;

create table fct_events
row format delimited
fields terminated by ','
stored as ORC
as
select fp.id,fp.event_id,substr(fp.event_time,1,7) as event_time,	fp.product_id,dp.price, fp.customer_id 
from fct_prod fp
left outer join dim_products dp on fp.product_id=dp.product_id;

select event_time, sum(price) from fct_events group by event_time; --query

insert into fct_events --append data
select fp.id,fp.event_id, substr(fp.event_time,1,7) as event_time, fp.product_id,dp.price, fp.customer_id from fct_prod fp 
join (select max(id) as mxid from fct_events) fe 
left outer join dim_products dp on fp.product_id=dp.product_id
where fp.id>fe.mxid;