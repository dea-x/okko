-- drop table DIM_SUPPLIERS
-- drop SEQUENCE suppliers_s

-- Создание последовательности для генерации индексов
CREATE SEQUENCE suppliers_s
	MINVALUE 1
	START WITH 1
	INCREMENT BY 1
	NOCACHE;
	
-- Создание таблицы
CREATE TABLE DIM_SUPPLIERS ( 
	suppliers_id NUMBER, 
	category VARCHAR2(25), 
	name VARCHAR2(40), 
	country VARCHAR2(40), 
	city VARCHAR2(40), 
	last_update_date TIMESTAMP 
);

-- Вставка 4 значений 
insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, 'Кухня', 'IKEA', 'Россия', 'Москва', systimestamp);
insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, 'Дом', 'Твой Дом', 'Россия', 'Санкт-Петербург', systimestamp);
insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, 'Красота', 'Lush', 'Россия', 'Волгоград', systimestamp);
insert into DIM_SUPPLIERS values (suppliers_s.NEXTVAL, 'Mobile', 'СитиЛинк', 'Россия', 'Тула', systimestamp);
commit;

-- select * from DIM_SUPPLIERS