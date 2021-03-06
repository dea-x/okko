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
