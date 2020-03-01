--статичная таблица с перечнем товаров
create table products (
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



--статичная таблица брэндов 
create table brands (brand VARCHAR2(30));
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


create table category (category_id number, category_code varchar2(25));
insert into category values (1, 'Дом');
insert into category values (2, 'Кухня');
insert into category values (3, 'Красота');
insert into category values (4, 'Mobile');


--генерируемая таблица товаров
CREATE TABLE dim_products ( 
    product_id         NUMBER, 
    category_id        NUMBER,     
    brand              VARCHAR2(30), 
    description        VARCHAR2(100), 
    name               VARCHAR2(50), 
    price              NUMBER,
    last_update_date   DATE
);
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