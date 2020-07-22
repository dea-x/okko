-- Наполняем справочник типы событий --

insert into DIM_EVENT_TYPE  values (1, 'view');
insert into DIM_EVENT_TYPE  values (2, 'cart');
insert into DIM_EVENT_TYPE  values (3, 'purchase');
insert into DIM_EVENT_TYPE  values (4, 'remove');
commit;
