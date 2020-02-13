--Таблица справочник имен--

CREATE TABLE DIM_NAMES (
id NUMBER,
name VARCHAR2(20),
gender VARCHAR(2)
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


--Таблица справочник фамилий--

 CREATE TABLE DIM_LASTNAMES (
id NUMBER,
name VARCHAR2(20),
gender VARCHAR(2)
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

--PL/SQL: часть для генерации справочника заказчиков--

declare
	c_id number;
	c_country VARCHAR2(100); 
	c_city VARCHAR2(100);  
	c_phone VARCHAR2(100); 
	c_fname VARCHAR2(100); 
	c_lname VARCHAR2(100); 
	c_mail VARCHAR2(100);
	c_last_update_date TIMESTAMP;
    
begin
 null;
 c_id := 1;
 while c_id <=20 loop
	c_country := 'Россия';
	select decode(abs(mod(DBMS_RANDOM.RANDOM, 5)), 0, 'Москва', 1, 'Санкт-Петербург', 2, 'Воронеж', 3, 'Мурманск', 4, 'Волгоград') into c_city from dual;
	select '8-'||decode(abs(mod(DBMS_RANDOM.RANDOM, 3)), 0, '903', 1, '909', '916')||'-'||to_char(mod(abs(DBMS_RANDOM.RANDOM), 8000000)+1000000) into c_phone from dual;
    if mod(mod(c_id, 0), 10) = 0 THEN
        select name into c_fname from (select name, dbms_random.value() rnd from dim_names where gender = 'ж' order by rnd) fetch first 1 rows only;
        select name into c_lname from (select name, dbms_random.value() rnd from dim_lastnames where gender = 'ж' order by rnd) fetch first 1 rows only;
    else 
        select name into c_fname from (select name, dbms_random.value() rnd from dim_names where gender = 'м' order by rnd) fetch first 1 rows only;
        select name into c_lname from (select name, dbms_random.value() rnd from dim_lastnames where gender = 'м' order by rnd) fetch first 1 rows only;
    end if;
	select DBMS_RANDOM.STRING('a', 6)||decode(abs(mod(DBMS_RANDOM.RANDOM, 3)), 0, '@gmail.com', 1, '@mail.ru', 2, '@ya.ru') into c_mail from dual;
	select CURRENT_TIMESTAMP into c_last_update_date from dual;

    insert into dim_customers values (c_id, c_country, c_city, c_phone, c_fname, c_lname, c_mail, c_last_update_date);
    
    c_id := c_id + 1;
    end loop;
    commit;
end;
--------------------------------------------------------
