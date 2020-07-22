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
                    if mod(c_id_st, 10) = 0 THEN
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
