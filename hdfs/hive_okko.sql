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
LOCATION '/user/usertest/MAI/dim_customers/'
tblproperties ("skip.header.line.count"="1");


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
LOCATION '/user/usertest/MAI/dim_products/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE dim_suppliers (
    suppliers_id INT, 
    category_id INT, 
    name STRING, 
    country STRING, 
    city STRING, last_update_date DATE, 
    primary key(suppliers_id) disable novalidate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/MAI/dim_suppliers/'
tblproperties ("skip.header.line.count"="1");


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
LOCATION '/user/usertest/MAI/fct_prod/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE dim_event_type (
    event_id INT, 
    event_type STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/usertest/okko/dim_event_type/'
tblproperties ("skip.header.line.count"="1");

select cast(substr(last_update_date,1,10) as date) from dim_customers; --парсинг времени

create table fct_events  --создание внутренней таблицы
row format delimited
fields terminated by ','
stored as ORC
as
select fp.id, fp.event_id, substr(fp.event_time,1,7) as event_time,	fp.product_id, dp.price, fp.customer_id 
from fct_prod fp
left outer join dim_products dp on fp.product_id=dp.product_id;

select event_time, sum(price) from fct_events group by event_time; --query




insert into fct_events --append data
select fp.id,fp.event_id, substr(fp.event_time,1,7) as event_time, fp.product_id,dp.price, fp.customer_id from fct_prod fp 
join (select max(id) as mxid from fct_events) fe 
left outer join dim_products dp on fp.product_id=dp.product_id
where fp.id>fe.mxid;





--Самые продаваемые товары по количеству
SELECT  product_id , COUNT(product_id) cnt
FROM fct_events
Where event_id = 4
GROUP BY product_id
ORDER BY cnt desc


--Товары по сумме, груп. по дню

SELECT substr(fp.event_time,1,10) day, 
SUM(dp.price) sold, YEAR(fp.event_time) year, 
WEEKOFYEAR(fp.event_time) weekofyear, 
MONTH (fp.event_time) month, 
QUARTER(fp.event_time) quarter 
FROM FCT_PROD fp, DIM_PRODUCTS dp 
WHERE event_id = 4 and fp.product_id=dp.product_id 
GROUP BY substr(fp.event_time,1,10), YEAR(fp.event_time), WEEKOFYEAR(fp.event_time), MONTH (fp.event_time), QUARTER(fp.event_time) 
ORDER BY day;
