# okko

Проект по эмуляции событий интернет-магазина на базе данных Oracle, пересылка сгенерированных событий в kafka producer, с последующим чтением событий kafka consumer и отправка их в другую базу Oracle, а также в Hdfs.

Задачи
----------------------------------------------------------------------------------------------------------------------------
task_1: Реализация данных исходной (source) базы Oracle. 

1.Создание БД, создание пользователя, предоставление ему прав
2. Создание таблиц и определение типов данных

Структура таблиц исходной БД:


**---Справочник Покупатели---**

CREATE TABLE DIM_CUSTOMERS ( 
customer_id NUMBER, 
country VARCHAR2(40), 
city VARCHAR2(40), 
phone VARCHAR2(40), 
first_name VARCHAR2(40), 
last_name VARCHAR2(40), 
mail VARCHAR2(50), 
last_update_date TIMESTAMP 
);


**---Справочник Поставщики---**

CREATE TABLE DIM_SUPPLIERS ( 
suppliers_id NUMBER, 
category VARCHAR2(25), 
name VARCHAR2(40), 
country VARCHAR2(40), 
city VARCHAR2(40), 
last_update_date TIMESTAMP 
);


**---Справочник Товары---** 

CREATE TABLE DIM_PRODUCTS ( 
product_id NUMBER, 
category_id NUMBER, 
category_code VARCHAR2(25), 
brand VARCHAR2(30), 
description VARCHAR2(100), 
name VARCHAR2(30), 
price NUMBER, 
last_update_date TIMESTAMP 
);


**---Таблица исторических событий---** *не используется в продюсере и консьюмере*

CREATE TABLE FCT_SHORT (
event_time DATE, 
event_type VARCHAR2(20), 
event_id NUMBER, 
product_id NUMBER,
customer_id NUMBER
);


**---Таблица событий (таблица фактов)---** 

CREATE TABLE FCT_EVENTS ( 
event_time TIMESTAMP, 
event_type VARCHAR2(20), 
event_id NUMBER, 
product_id NUMBER, 
category_id NUMBER, 
category_code VARCHAR2(25), 
brand VARCHAR2(30), 
price NUMBER, 
customer_id NUMBER,  
CONSTRAINT event_pk PRIMARY KEY (event_id) 
)
as 
(select cast(sh.event_time as timestamp) as event_time, sh.event_type, sh.event_id, sh.product_id, pr.category_id,
       pr.category_code, pr.brand, pr.price,
       sh.customer_id
       from fct_short sh
       join dim_products pr on sh.product_id = pr.product_id);


3. Наполнение исходных таблиц посредством PL/SQL пакета
4. Добавить job, для запуска пакета по расписанию, через dbms_job (генерация раз в 5-10 минут)

----------------------------------------------------------------------------------------------------------------------------
task_2: Написать producer kafka (producer_ok.py)

1. Создать на сервере kafka топик 'okko' 
2. Написать producer kafka на python (прописать коннект к source базе Oraclе, указать топик куда загружаем данные 'okko'). Продюсер должен производить инкрементную загрузку из таблиц Oracle в топик kafka. Справочники переносить через last update date, события магазина через id (они изменяться не могут)
3. Средствами Oozie Coordinator добавить задание на запуск producer_ok.py, с интервалом раз в три минуты.		

----------------------------------------------------------------------------------------------------------------------------
task_3: Написать consumer kafka (consumer_ko.py)

1. Создать на сервере в hdfs новый каталог 'okko' (hadoop fs -mkdir /user/usertest/okko)
2. Написать consumer kafka, который будет переносить таблицы в target базу Oracle и параллельно в hdfs. Оба покота в одном консьюмере. 
3. Средствами Oozie Coordinator добавить задание на запуск consumer_ko.py

----------------------------------------------------------------------------------------------------------------------------
task_4: Оптимизация и быстродействие исходной базы Oracle

1. Нужно попробовать создать таблицу с партицированием.
2. Рассмотреть вопрос, какие нужно поставить индексы (если это нужно).
3. Проверить скорость работы, через sql_trace и далее через tkprof.

----------------------------------------------------------------------------------------------------------------------------
task_5:  Рассчитываем аналитику в результирующей базе Oracle

В результирующей базе Oracle создать materialized view on commit с аналитикой по продажам
цифры по продажам за день, неделю, месяц и год.

----------------------------------------------------------------------------------------------------------------------------
task_6: Рассчитываем аналитику в hdfs

В hdfs рассчитываем аналитику по продажам: самые часто продаваемые товары по периодам.

----------------------------------------------------------------------------------------------------------------------------
task_7: В исходной и конечной базах Oracle, реализовать схему удаления "старых" данных

В source и target базах Oracle, можно продумать схему по удалению "старых" данных и хранить данные, скажем только за год. 
При увеличении потока данных такая проблема может всплыть (исчерпается диск в Oracle). Все данные по классике у нас должны остаться только в hdfs. 
Реализовать "очистку" можно, например, через pl/sql-пакет и dbms_job.
