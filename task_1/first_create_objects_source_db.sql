-- Первоначальное создание всей структуры исходной БД проекта OKKO -- 

-- Таблица Логов --

CREATE TABLE log_table (
    time_log            TIMESTAMP,
    level_log           VARCHAR2(10),
    program_name        VARCHAR2(40),
    procedure_name      VARCHAR2(40),
    message             VARCHAR2(1000)
);

--------------------------------------------------------

-- Последовательность для id таблицы Фактов --

CREATE SEQUENCE fct_s_prod
    MINVALUE 1
    START WITH 1
    INCREMENT BY 1
    NOCACHE;
	
--------------------------------------------------------

-- Подсправочник имен --

CREATE TABLE DIM_NAMES (
	id 		NUMBER,
	name 	VARCHAR2(20),
	gender 	VARCHAR(2)
);

insert into DIM_NAMES VALUES (1, 'Владислав', 'м');
insert into DIM_NAMES VALUES (2, 'Семен', 'м');
insert into DIM_NAMES VALUES (3, 'Евгений', 'м');
insert into DIM_NAMES VALUES (4, 'Владимир', 'м');
insert into DIM_NAMES VALUES (5, 'Елена', 'ж');
insert into DIM_NAMES VALUES (6, 'Анатолий', 'м');
insert into DIM_NAMES VALUES (7, 'Андрей', 'м');
insert into DIM_NAMES VALUES (8, 'Светлана', 'ж');
insert into DIM_NAMES VALUES (9, 'Вадим', 'м');
insert into DIM_NAMES VALUES (10, 'Петр', 'м');
insert into DIM_NAMES VALUES (11, 'Илья', 'м');
insert into DIM_NAMES VALUES (12, 'Ирина', 'ж');
insert into DIM_NAMES VALUES (13, 'Виктория', 'ж');
insert into DIM_NAMES VALUES (14, 'Ольга', 'ж');
insert into DIM_NAMES VALUES (15, 'Денис', 'м');
insert into DIM_NAMES VALUES (16, 'Артем', 'м');
insert into DIM_NAMES VALUES (17, 'Людмила', 'ж');
insert into DIM_NAMES VALUES (18, 'Антон', 'м');
insert into DIM_NAMES VALUES (19, 'Дмитрий', 'м');
insert into DIM_NAMES VALUES (20, 'Степан', 'м');

commit;

--------------------------------------------------------

-- Подсправочник  фамилий --

CREATE TABLE DIM_LASTNAMES (
	id 		NUMBER,
	name 	VARCHAR2(20),
	gender 	VARCHAR(2)
);

insert into DIM_LASTNAMES VALUES (1, 'Иванов', 'м');
insert into DIM_LASTNAMES VALUES (2, 'Петров', 'м');
insert into DIM_LASTNAMES VALUES (3, 'Сидоров', 'м');
insert into DIM_LASTNAMES VALUES (4, 'Калинин', 'м');
insert into DIM_LASTNAMES VALUES (5, 'Трубная', 'ж');
insert into DIM_LASTNAMES VALUES (6, 'Тихонов', 'м');
insert into DIM_LASTNAMES VALUES (7, 'Аксенов', 'м');
insert into DIM_LASTNAMES VALUES (8, 'Назарова', 'ж');
insert into DIM_LASTNAMES VALUES (9, 'Конев', 'м');
insert into DIM_LASTNAMES VALUES (10, 'Ветров', 'м');
insert into DIM_LASTNAMES VALUES (11, 'Баринов', 'м');
insert into DIM_LASTNAMES VALUES (12, 'Степанова', 'ж');
insert into DIM_LASTNAMES VALUES (13, 'Березова', 'ж');
insert into DIM_LASTNAMES VALUES (14, 'Немина', 'ж');
insert into DIM_LASTNAMES VALUES (15, 'Крутов', 'м');
insert into DIM_LASTNAMES VALUES (16, 'Кротов', 'м');
insert into DIM_LASTNAMES VALUES (17, 'Павлова', 'ж');
insert into DIM_LASTNAMES VALUES (18, 'Варламов', 'м');
insert into DIM_LASTNAMES VALUES (19, 'Харонов', 'м');
insert into DIM_LASTNAMES VALUES (20, 'Кисилев', 'м');

commit;

--------------------------------------------------------

-- Справочник Покупатели --

CREATE TABLE DIM_CUSTOMERS ( 
	customer_id 		NUMBER, 
	country 			VARCHAR2(40), 
	city 				VARCHAR2(40), 
	phone 				VARCHAR2(40), 
	first_name 			VARCHAR2(40), 
	last_name 			VARCHAR2(40), 
	mail 				VARCHAR2(50), 
	last_update_date 	DATE,
	CONSTRAINT customer_pk PRIMARY KEY (customer_id)
);

-- Первоначальная генерации данных для справочника покупателей --
declare
	c_id_st 			number;
    c_id_end 			number;
	c_country 			VARCHAR2(40); 
	c_city 				VARCHAR2(40);  
	c_phone 			VARCHAR2(40); 
	c_fname 			VARCHAR2(40); 
	c_lname 			VARCHAR2(40); 
	c_mail 				VARCHAR2(50);
	c_last_update_date 	DATE;
    
begin
    -- выбираем максимальный итерируемый id    
    select max(customer_id) into c_id_st from dim_customers;
    	-- проверяем не пустая ли таблица, что бы не словить null в итератор
    	if c_id_st IS NULL 
		then  c_id_st:=1; 
    	end if;
    -- выставляем количество шагов итерации
    c_id_end := c_id_st + 20;
    
	   -- выполяем итерацию пока
            while c_id_st <= c_id_end loop
                c_country := 'Россия';
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
end;

--------------------------------------------------------

-- Подсправочник  Продуктов --

CREATE TABLE  products (
    product        VARCHAR2(50),
    des            VARCHAR2(100),
    category_id    NUMBER
);


insert into products (product, des, category_id) values ('Кофеварка', 'Рожковая', 2);
insert into products (product, des, category_id) values ('Кофеварка', 'Капельная', 2);
insert into products (product, des, category_id) values ('Кофеварка', 'Капсульная', 2);
insert into products (product, des, category_id) values ('СВЧ печь', 'СВЧ', 2);
insert into products (product, des, category_id) values ('Чайник', 'Стекло', 2);
insert into products (product, des, category_id) values ('Чайник', 'Пластик', 2);
insert into products (product, des, category_id) values ('Блендер', 'Набор', 2);
insert into products (product, des, category_id) values ('Мясорубка', 'Многофункциональный', 2);
insert into products (product, des, category_id) values ('ТВ', 'OLED', 1);
insert into products (product, des, category_id) values ('ТВ', 'QLED', 1);
insert into products (product, des, category_id) values ('Аудио-система', '5.1', 1);
insert into products (product, des, category_id) values ('Аудио-система', '7.1', 1);
insert into products (product, des, category_id) values ('Утюг', 'С парогенератором', 1);
insert into products (product, des, category_id) values ('Утюг', 'Без парогенератора', 1);
insert into products (product, des, category_id) values ('Пылесос', 'Мешковый', 1);
insert into products (product, des, category_id) values ('Пылесос', 'Безмешковый', 1);
insert into products (product, des, category_id) values ('Триммер', 'Проводной', 3);
insert into products (product, des, category_id) values ('Триммер', 'Безпроводной', 3);
insert into products (product, des, category_id) values ('Зубная щетка', 'Набор', 3);
insert into products (product, des, category_id) values ('Зубная щетка', 'Одна', 3);
insert into products (product, des, category_id) values ('Фен', 'Профессиональный', 3);
insert into products (product, des, category_id) values ('Фен', 'Набор', 3);
insert into products (product, des, category_id) values ('Стайлер', 'Набор', 3);
insert into products (product, des, category_id) values ('Смартфон', '4inch', 4);
insert into products (product, des, category_id) values ('Смартфон', '5inch', 4);
insert into products (product, des, category_id) values ('Колонка', '3Вт', 4);
insert into products (product, des, category_id) values ('Колонка', '5Вт', 4);
insert into products (product, des, category_id) values ('Смарт-часы', 'Резина', 4);
insert into products (product, des, category_id) values ('Смарт-часы', 'Текстиль', 4);
insert into products (product, des, category_id) values ('Фотоаппарат', 'Зеркальный', 4);
insert into products (product, des, category_id) values ('Фотоаппарат', 'Компактный', 4);
insert into products (product, des, category_id) values ('Пылесос', 'Вертикальный', 1);
insert into products (product, des, category_id) values ('Пылесос', 'Робот-пылесос', 1);
insert into products (product, des, category_id) values ('Увлажнитель', 'С очистителем', 1);
insert into products (product, des, category_id) values ('Обогреватель', 'Радиаторный', 1);
insert into products (product, des, category_id) values ('Электробритва', 'Роторная', 3);
insert into products (product, des, category_id) values ('Холодильник', '2 камеры', 2);
insert into products (product, des, category_id) values ('Холодильник', '1 камера', 2);
insert into products (product, des, category_id) values ('Стиральная машина', 'Встраиваемая', 2);
insert into products (product, des, category_id) values ('Стиральная машина', 'Компакт', 1);
insert into products (product, des, category_id) values ('Духовой шкаф', 'Встраиваемый', 2);
insert into products (product, des, category_id) values ('Духовой шкаф', 'Переносной', 1);
insert into products (product, des, category_id) values ('Принтер', 'Струйный', 4);
insert into products (product, des, category_id) values ('Принтер', 'Лазерный', 4);
insert into products (product, des, category_id) values ('Принтер', 'МФУ', 4);
insert into products (product, des, category_id) values ('Тостер', 'Черный', 2);
insert into products (product, des, category_id) values ('Тостер', 'Белый', 2);
insert into products (product, des, category_id) values ('Эпилятор', 'Набор', 3);
insert into products (product, des, category_id) values ('Машинка для стрижки', 'Набор', 3);
insert into products (product, des, category_id) values ('Мультиварка', 'Белая', 2);
insert into products (product, des, category_id) values ('ТВ', '4К', 1);
insert into products (product, des, category_id) values ('Монитор', '20inch', 4);
insert into products (product, des, category_id) values ('Монитор', '25inch', 4);
insert into products (product, des, category_id) values ('Планшет', '9inch', 4);
insert into products (product, des, category_id) values ('Светильник', 'LED', 1);
insert into products (product, des, category_id) values ('Светильник', '40ВТ', 1);
insert into products (product, des, category_id) values ('Светильник', '15ВТ', 1);
insert into products (product, des, category_id) values ('Кондиционер', 'Инвентор', 1);
insert into products (product, des, category_id) values ('Кондиционер', 'Напольный', 1);
insert into products (product, des, category_id) values ('Видеокамера', '4К', 4);
insert into products (product, des, category_id) values ('Видеокамера', 'FullHD', 4);


commit;

--------------------------------------------------------

-- Подсправочник  Брендов --

CREATE TABLE brands (
	brand 	VARCHAR2(30)
);

insert into brands (brand) values ('LG');
insert into brands (brand) values ('Samsung');
insert into brands (brand) values ('Philips');
insert into brands (brand) values ('Braun');
insert into brands (brand) values ('Bosch');
insert into brands (brand) values ('Vitek');
insert into brands (brand) values ('Sony');
insert into brands (brand) values ('Supra');
insert into brands (brand) values ('Electrolux');
insert into brands (brand) values ('Dyson');
insert into brands (brand) values ('Xiaomi');
insert into brands (brand) values ('Siemens');
insert into brands (brand) values ('Daewoo');
insert into brands (brand) values ('Miele');
insert into brands (brand) values ('Polaris');

--------------------------------------------------------

-- Справочник Категорий --

CREATE TABLE CATEGORY ( 
	category_id 	NUMBER, 
	category_code 	VARCHAR2(25), 
	CONSTRAINT category_pk PRIMARY KEY (category_id)
);

drop table dim_category;
insert into category values (1, 'Дом');
insert into category values (2, 'Кухня');
insert into category values (3, 'Красота');
insert into category values (4, 'Mobile');

--------------------------------------------------------

-- Справочник Товары --

CREATE TABLE DIM_PRODUCTS ( 
	product_id 			NUMBER, 
	category_id 		NUMBER, 
	brand 				VARCHAR2(30), 
	description 		VARCHAR2(100), 
	name 				VARCHAR2(50), 
	price 				NUMBER, 
	last_update_date 	DATE, 
	CONSTRAINT product_pk PRIMARY KEY (product_id)
);

-- Первоначальная генерации данных для справочника товаров --

declare 
  bins number;
  r    number;

begin
--при создании задаем большое число товаров; если таблица существует, объявляем инкремент
    select count(*)	+1 into r from dim_products;
    if (r<11) then 
        bins := 10;
    else 
        bins := 2;
    end if;
    for i in 1..bins
        loop 
        insert into dim_products (description, name, product_id, category_id, brand, price, last_update_date)
            (select ft.des, ft.product, --description, name
            (select count(*)+1 from dim_products), --product_id
            ft.category_id, --category_id
            (select brand from --brand
            (select brand, dbms_random.value() rnd from brands order by rnd) fetch first 1 rows only),
            round(dbms_random.value(2000, 10000)),
            SYSDATE
        from 
            (select category_id, product, des, dbms_random.value(1, (select count(*) from products)) rd 
            from products order by rd) ft fetch first 1 rows only
        );
    end loop;
    commit;
end;

--------------------------------------------------------

-- Последовательность для id таблицы Поставщики --

CREATE SEQUENCE suppliers_s
	MINVALUE 1
	START WITH 1
	INCREMENT BY 1
	NOCACHE;

--------------------------------------------------------

-- Справочник Поставщики --

CREATE TABLE DIM_SUPPLIERS ( 
	suppliers_id NUMBER, 
	category_id NUMBER, 
	name VARCHAR2(40), 
	country VARCHAR2(40), 
	city VARCHAR2(40), 
	last_update_date DATE,
	CONSTRAINT suppliers_pk PRIMARY KEY (suppliers_id)
);

-- Первоначальная генерации данных для справочника поставщиков --
declare
    -- создание массива для хранения городов
    type arr_type_city is table of varchar2(100)
    index by binary_integer;
  
	category_id         NUMBER;
	name 				VARCHAR2(40); 
	country				VARCHAR2(40); 
	city				varchar2(40);
    cities     			arr_type_city;
	city_index			number;
	flag				number(1);
 
begin
    cities(1) := 'Москва';
    cities(2) := 'Санкт-Петербург';
    cities(3) := 'Волгоград';
    cities(4) := 'Калининград';
    cities(5) := 'Астрахань';
    cities(6) := 'Тула';
    
    country := 'Россия';
	
	-- получение всех доступных кодов категорий
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
        end loop;
        commit;
end;

--------------------------------------------------------

-- Справочник События --

CREATE TABLE DIM_EVENT_TYPE (
    event_id     NUMBER,
    event_type   VARCHAR2(20), 
	CONSTRAINT event_id_pk PRIMARY KEY (event_id)
);  

insert into DIM_EVENT_TYPE  values (1, 'view');
insert into DIM_EVENT_TYPE  values (2, 'cart');
insert into DIM_EVENT_TYPE  values (3, 'purchase');
insert into DIM_EVENT_TYPE  values (4, 'remove');
commit;

--------------------------------------------------------

-- Таблица Фактов --

CREATE TABLE FCT_PROD (
	id 			 NUMBER,
	event_id	 NUMBER,
    event_time   DATE, 
    product_id   NUMBER,
    customer_id  NUMBER,
	CONSTRAINT id_pk PRIMARY KEY (id)
);

-- Первоначальная генерации данных для таблицы Фактов --

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
                (fct_s_prod.NEXTVAL,  --id события 
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

--------------------------------------------------------
		      
