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
