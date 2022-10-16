-- create tables
create table customer (
    custkey        int,
    orderkey       int
);

create table orders (
    orderkey       int
);

-- insert data
INSERT INTO customer 
    WITH RECURSIVE 
    cte(x, y) AS ( 
    VALUES (abs(random()) % 10, abs(random())%10) UNION ALL 
    SELECT abs(random()) % 10, abs(random())%10 FROM cte limit 10) 
    select * from cte;

INSERT INTO orders 
    WITH RECURSIVE 
    cte(x) AS ( 
    VALUES(random() % 10) UNION ALL 
    SELECT random() % 10 FROM cte limit 10) 
    select * from cte;

select * from customer;
select * from orders;
delete from customer;
delete from orders;
-- join
select customer.custkey, customer.orderkey, orders.orderkey  from customer, orders where customer.custkey = orders.orderkey;
