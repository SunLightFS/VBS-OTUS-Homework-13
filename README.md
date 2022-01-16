# Задание №13: Триггеры, поддержка заполнения витрин

### Задание

```
-- ДЗ тема: триггеры, поддержка заполнения витрин

DROP SCHEMA IF EXISTS pract_functions CASCADE;
CREATE SCHEMA pract_functions;

SET search_path = pract_functions, publ

-- товары:
CREATE TABLE goods
(
    goods_id    integer PRIMARY KEY,
    good_name   varchar(63) NOT NULL,
    good_price  numeric(12, 2) NOT NULL CHECK (good_price > 0.0)
);
INSERT INTO goods (goods_id, good_name, good_price)
VALUES 	(1, 'Спички хозайственные', .50),
		(2, 'Автомобиль Ferrari FXX K', 185000000.01);

-- Продажи
CREATE TABLE sales
(
    sales_id    integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    good_id     integer REFERENCES goods (goods_id),
    sales_time  timestamp with time zone DEFAULT now(),
    sales_qty   integer CHECK (sales_qty > 0)
);

INSERT INTO sales (good_id, sales_qty) VALUES (1, 10), (1, 1), (1, 120), (2, 1);

-- отчет:
SELECT G.good_name, sum(G.good_price * S.sales_qty)
FROM goods G
INNER JOIN sales S ON S.good_id = G.goods_id
GROUP BY G.good_name;

-- с увеличением объёма данных отчет стал создаваться медленно
-- Принято решение денормализовать БД, создать таблицу
CREATE TABLE good_sum_mart
(
	good_name   varchar(63) NOT NULL,
	sum_sale	numeric(16, 2)NOT NULL
);

-- Создать триггер (на таблице sales) для поддержки.
-- Подсказка: не забыть, что кроме INSERT есть еще UPDATE и DELETE

-- Чем такая схема (витрина+триггер) предпочтительнее отчета, создаваемого "по требованию" (кроме производительности)?
-- Подсказка: В реальной жизни возможны изменения цен.


```

### Выполнение

В первую очередь нужно перенести имеющиеся данные в новую таблицу:
```
insert into good_sum_mart	
	select g.good_name, sum(g.good_price * g.sales_qty)
		from goods g
		    inner join sales s ON s.good_id = g.goods_id
	group by g.good_name;
```
В результате select даст нам следующие данные:
```
otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Автомобиль Ferrari FXX K | 185000000.01
 Спички хозайственные     |        65.50
(2 rows)
```
Далее создаем триггерную функцию, которая будет отвечать за обновление данных в таблице good_sum_mart:
```
create or replace function change_sale_data()
returns trigger
AS $function$
        declare
                good_name_var varchar(63);
                good_price_var numeric(12, 2);
                old_sum numeric(16, 2);
                new_sum numeric(16, 2);
        begin
                select goods.good_name, goods.good_price into good_name_var, good_price_var from goods where goods.goods_id = coalesce(new.good_id, old.good_id);
                select sum_sale into old_sum from good_sum_mart where good_name = good_name_var;

                if (TG_OP = 'INSERT') then
                        if (select good_name_var in (select good_name from good_sum_mart)) then
                                new_sum = old_sum + good_price_var * new.sales_qty;
                                update good_sum_mart set sum_sale = new_sum where good_name = good_name_var;
                        else
                                insert into good_sum_mart values (good_name_var, good_price_var * new.sales_qty);
                        end if;
                elsif (TG_OP = 'UPDATE') then
                        new_sum = old_sum - good_price_var * old.sales_qty + good_price_var * new.sales_qty;
                        update good_sum_mart set sum_sale = new_sum where good_name = good_name_var;
                elsif (TG_OP = 'DELETE') then
                        new_sum = old_sum - good_price_var * old.sales_qty;
                        if new_sum > 0 then
                                update good_sum_mart set sum_sale = new_sum where good_name = good_name_var;
                        else
                                delete from good_sum_mart where good_name = good_name_var;
                        end if;
                end if;
                return null;
        end;
        $function$
language plpgsql;
```
И сам триггер:
```
create trigger change_sale_data
    after insert or update or delete
    on sales for each row
    execute procedure change_sale_data();
```
Теперь проверим работу триггера в различных ситуациях:  
1) Добавление записи в sales с уже имеющимся в good_sum_mart продуктом обновит существующую в таблице строку:
```
insert into sales (good_id, sales_qty) values (1, 3);

otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Автомобиль Ferrari FXX K | 185000000.01
 Спички хозайственные     |        67.00
(2 rows)
```

2) Добавление записи в sales с новым продуктом, которого пока нет в good_sum_mart создаст в данной таблице строку с этим продуктом:
```
-- Добавим новый продукт в goods
insert into goods (goods_id, good_name, good_price) values (3, 'Нечто крайне полезное', 1000);

-- Теперь добавим в sales продажу этого продукта
insert into sales (good_id, sales_qty) values (3, 5);

-- В результате получим новую строку в good_sum_mart
otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Автомобиль Ferrari FXX K | 185000000.01
 Спички хозайственные     |        67.00
 Нечто крайне полезное    |      5000.00
(3 rows)
```

3) Обновление записи в sales повлечет соответствующее изменение итоговой суммы в good_sum_mart:
```
-- Посмотрим на текущее содержимое таблицы sales
 sales_id | good_id |          sales_time           | sales_qty
----------+---------+-------------------------------+-----------
        1 |       1 | 2022-01-16 05:21:24.754235+00 |        10
        2 |       1 | 2022-01-16 05:21:24.754235+00 |         1
        3 |       1 | 2022-01-16 05:21:24.754235+00 |       120
        4 |       2 | 2022-01-16 05:21:24.754235+00 |         1
        5 |       1 | 2022-01-16 05:22:10.507671+00 |         3
        6 |       3 | 2022-01-16 05:39:25.087232+00 |         5
(6 rows)

-- Изменим количество купленых спичек в первой строке с 10 на 2
update sales set sales_qty = 2 where sales_id = 1;

-- Теперь в good_sum_mart значение суммы у спичек должно уменьшиться на 4 (т.к. мы уменьшили количество проданных спичек на 8 шт.):
otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Автомобиль Ferrari FXX K | 185000000.01
 Нечто крайне полезное    |      5000.00
 Спички хозайственные     |        63.00
(3 rows)
```

4) Удаление строки с продажей из sales при наличии других строк с этим продуктом приведет к уменьшению суммарной стоимости в good_sum_mart:
```
-- Удалим все ту же строку с id = 1 из таблицы sales:
delete from sales where sales_id = 1;

-- Теперь в good_sum_mart значение суммы у спичек должно уменьшиться на 1 (т.к. мы уменьшили суммарное количество проданных спичек еще на 2 шт.):
otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Автомобиль Ferrari FXX K | 185000000.01
 Нечто крайне полезное    |      5000.00
 Спички хозайственные     |        62.00
(3 rows)
```

5) Удаление единственной продажи продукта из sales приведет к удалению строки с этим продуктом из good_sum_mart:
```
-- Удалим из таблицы единственную продажу автомобиля:
delete from sales where sales_id = 4;

-- Соответствующая строка из таблицы good_sum_mart также была удалена:
otus=# select * from good_sum_mart;
        good_name         |   sum_sale
--------------------------+--------------
 Нечто крайне полезное    |      5000.00
 Спички хозайственные     |        62.00
(2 rows)
```
