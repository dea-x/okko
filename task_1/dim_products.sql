--статичная таблица с перечнем товаров
create table products (
    product         VARCHAR2(50),
    des             VARCHAR2(100),
    category_code   VARCHAR2(25)
);

insert into products (product, des, category_code) values ('Кофеварка', 'Рожковая', 'Кухня');
insert into products (product, des, category_code) values ('Кофеварка', 'Капельная', 'Кухня');
insert into products (product, des, category_code) values ('Кофеварка', 'Капсульная', 'Кухня');
insert into products (product, des, category_code) values ('СВЧ печь', 'СВЧ', 'Кухня');
insert into products (product, des, category_code) values ('Чайник', 'Стекло', 'Кухня');
insert into products (product, des, category_code) values ('Чайник', 'Пластик', 'Кухня');
insert into products (product, des, category_code) values ('Блендер', 'Набор', 'Кухня');
insert into products (product, des, category_code) values ('Мясорубка', 'Многофункциональный', 'Кухня');
insert into products (product, des, category_code) values ('ТВ', 'OLED', 'Дом');
insert into products (product, des, category_code) values ('ТВ', 'QLED', 'Дом');
insert into products (product, des, category_code) values ('Аудио-система', '5.1', 'Дом');
insert into products (product, des, category_code) values ('Аудио-система', '7.1', 'Дом');
insert into products (product, des, category_code) values ('Утюг', 'С парогенератором', 'Дом');
insert into products (product, des, category_code) values ('Утюг', 'Без парогенератора', 'Дом');
insert into products (product, des, category_code) values ('Пылесос', 'Мешковый', 'Дом');
insert into products (product, des, category_code) values ('Пылесос', 'Безмешковый', 'Дом');
insert into products (product, des, category_code) values ('Триммер', 'Проводной', 'Красота');
insert into products (product, des, category_code) values ('Триммер', 'Безпроводной', 'Красота');
insert into products (product, des, category_code) values ('Зубная щетка', 'Набор', 'Красота');
insert into products (product, des, category_code) values ('Зубная щетка', 'Одна', 'Красота');
insert into products (product, des, category_code) values ('Фен', 'Профессиональный', 'Красота');
insert into products (product, des, category_code) values ('Фен', 'Набор', 'Красота');
insert into products (product, des, category_code) values ('Стайлер', 'Набор', 'Красота');
insert into products (product, des, category_code) values ('Смартфон', '4inch', 'Mobile');
insert into products (product, des, category_code) values ('Смартфон', '5inch', 'Mobile');
insert into products (product, des, category_code) values ('Колонка', '3Вт', 'Mobile');
insert into products (product, des, category_code) values ('Колонка', '5Вт', 'Mobile');
insert into products (product, des, category_code) values ('Смарт-часы', 'Резина', 'Mobile');
insert into products (product, des, category_code) values ('Смарт-часы', 'Текстиль', 'Mobile');
insert into products (product, des, category_code) values ('Фотоаппарат', 'Зеркальный', 'Mobile');
insert into products (product, des, category_code) values ('Фотоаппарат', 'Компактный', 'Mobile');
insert into products (product, des, category_code) values ('Пылесос', 'Вертикальный', 'Дом');
insert into products (product, des, category_code) values ('Пылесос', 'Робот-пылесос', 'Дом');
insert into products (product, des, category_code) values ('Увлажнитель', 'С очистителем', 'Дом');
insert into products (product, des, category_code) values ('Обогреватель', 'Радиаторный', 'Дом');
insert into products (product, des, category_code) values ('Электробритва', 'Роторная', 'Красота');
insert into products (product, des, category_code) values ('Холодильник', '2 камеры', 'Кухня');
insert into products (product, des, category_code) values ('Холодильник', '1 камера', 'Кухня');
insert into products (product, des, category_code) values ('Стиральная машина', 'Встраиваемая', 'Кухня');
insert into products (product, des, category_code) values ('Стиральная машина', 'Компакт', 'Дом');
insert into products (product, des, category_code) values ('Духовой шкаф', 'Встраиваемый', 'Кухня');
insert into products (product, des, category_code) values ('Духовой шкаф', 'Переносной', 'Дом');
insert into products (product, des, category_code) values ('Принтер', 'Струйный', 'Mobile');
insert into products (product, des, category_code) values ('Принтер', 'Лазерный', 'Mobile');
insert into products (product, des, category_code) values ('Принтер', 'МФУ', 'Mobile');
insert into products (product, des, category_code) values ('Тостер', 'Черный', 'Кухня');
insert into products (product, des, category_code) values ('Тостер', 'Белый', 'Кухня');
insert into products (product, des, category_code) values ('Эпилятор', 'Набор', 'Красота');
insert into products (product, des, category_code) values ('Машинка для стрижки', 'Набор', 'Красота');
insert into products (product, des, category_code) values ('Мультиварка', 'Белая', 'Кухня');
insert into products (product, des, category_code) values ('ТВ', '4К', 'Дом');
insert into products (product, des, category_code) values ('Монитор', '20inch', 'Mobile');
insert into products (product, des, category_code) values ('Монитор', '25inch', 'Mobile');
insert into products (product, des, category_code) values ('Планшет', '9inch', 'Mobile');
insert into products (product, des, category_code) values ('Светильник', 'LED', 'Дом');
insert into products (product, des, category_code) values ('Светильник', '40ВТ', 'Дом');
insert into products (product, des, category_code) values ('Светильник', '15ВТ', 'Дом');
insert into products (product, des, category_code) values ('Кондиционер', 'Инвентор', 'Дом');
insert into products (product, des, category_code) values ('Кондиционер', 'Напольный', 'Дом');
insert into products (product, des, category_code) values ('Видеокамера', '4К', 'Mobile');
insert into products (product, des, category_code) values ('Видеокамера', 'FullHD', 'Mobile');



--статичная таблица брэндов 
create table brands (brand VARCHAR2(30))
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


--генерируемая таблица товаров
CREATE TABLE dim_products ( 
    product_id         NUMBER, 
    category_id        NUMBER, 	
    category_code      VARCHAR2(25), 
    brand              VARCHAR2(30), 
    description        VARCHAR2(100), 
    name               VARCHAR2(50), 
    price              NUMBER,
    last_update_date   DATE
);

-- наполнение таблицы товаров
declare 
  bins number;
  r    number;

begin
--при создании задаем большое число товаров; если таблица существует, объявляем инкремент
    select count(*)	+1 into r from dim_products;
    if (r<11) then 
        bins := 10;
    else 
        bins :=2;
    end if;
    for i in 1..bins
        loop 
        insert into dim_products (category_code, description, name, product_id, category_id, brand, price, last_update_date)
            (select ft.category_code, ft.des, ft.product,
            (select count(*)+1 from dim_products),
            (select decode(category_code, 'Дом', 1, 'Кухня', 2, 'Красота', 3, 'Mobile', 4) from dual), 
            (select brand from 
            (select brand, dbms_random.value() rnd from brands order by rnd) fetch first 1 rows only),
            round(dbms_random.value(2000, 10000)),
            SYSDATE
        from 
            (select category_code, product, des, dbms_random.value(1, (select count(*) from products)) rd 
            from products order by rd) ft fetch first 1 rows only
        );
    end loop;
    commit;
end;