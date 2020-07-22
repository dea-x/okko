-- Инкрементная загрузка данных в таблицу фактов

declare
    rdn         number;
    prod_id     number;
    cust_id     number;
    temp        DATE;  
    delta       number;
	l_col_p     number;
    l_col_c     number;
  
begin  
    select SYSDATE into temp from dual; --инициализация текущего времени;
    select max(product_id)+1 into l_col_p from dim_products; --число строк в таблице товаров
	select max(customer_id)+1 into l_col_c from dim_customers;--число строк в таблице клиентов    
    rdn :=dbms_random.value(10,15); --случайное количество транзакций за 5 мин
    delta := 5*60/rdn; --среднее время в сек между транзакциями     
    for i in 1..rdn
    loop
        select (trunc(dbms_random.value(1, l_col_p))) into prod_id from dual; --trunc вместо round для оптимизации запроса
        select (trunc(dbms_random.value(1, l_col_c))) into cust_id from dual;		
		insert into fct_prod (id, event_id, event_time, product_id, customer_id) values
                (fct_s.NEXTVAL,  --id события 
                decode(trunc(dbms_random.value(1,10)), 1, 1, 2, 1, 3, 1, 4, 1,
                                                       5, 2, 6, 2, 7, 2,
                                                       8, 3,
                                                       9, 4), --вероятность событий
                temp,           
                trunc(dbms_random.value(1, l_col_p)), -- product_id
                trunc(dbms_random.value(1, l_col_c))  -- product_id
                );
		temp := temp + numToDSInterval(delta, 'second');  --инкрементация текущего времени;	       
    end loop;      
    commit;
end;
