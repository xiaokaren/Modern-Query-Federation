import os
import sqlite3

db_filename = 'todo.db'

def create_db():
    table_ddl = """
    create table customer (
        custkey        int
    );

    create table orders (
        orderkey       int
    );"""

    db_is_new = not os.path.exists(db_filename)

    conn = sqlite3.connect(db_filename)

    if db_is_new:
        print('Need to create schema')
        conn.executescript(table_ddl)

    else:
        print('Database exists')

    conn.close()

def load_data(n):
    clear_data = "delete from customer; delete from orders;"
    insert_query_customer = """INSERT INTO customer 
        WITH RECURSIVE 
        cte(x) AS ( 
        VALUES (abs(random()) % {num}) UNION ALL 
        SELECT abs(random()) % {num} FROM cte limit {num}) 
        select * from cte;""".format(num = n)
    insert_query_orders = """INSERT INTO orders 
        WITH RECURSIVE 
        cte(x) AS ( 
        VALUES (abs(random()) % {num}) UNION ALL 
        SELECT abs(random()) % {num} FROM cte limit {num}) 
        select * from cte;""".format(num = n)
    conn = sqlite3.connect(db_filename)
    conn.executescript(clear_data)
    conn.executescript(insert_query_customer)
    conn.executescript(insert_query_orders)


def query_data(query):
    conn = sqlite3.connect(db_filename)

    cursor = conn.execute(query)
    result = []
    for row in cursor:
        result.append(row)
        print(row)
    return len(result)

def join_all_on_sqllite():
    query = "select customer.custkey,  orders.orderkey  from customer, orders where customer.custkey = orders.orderkey;"
    print(query_data(query))


def join_on_mini_trino():
    conn = sqlite3.connect(db_filename)
    get_customer_data_query = "select custkey from customer"
    get_orders_data_query = "select orderkey from orders"

    cursor = conn.execute(get_customer_data_query)
    customer_data = []
    orders_data = []
    for row in cursor:
        customer_data.append(row)
    
    cursor = conn.execute(get_orders_data_query)
    for row in cursor:
        orders_data.append(row)


    # join customer's first column with orders' first column, could be a nested loop join, hash join, sort merge join etc.
    
    #joined_data = nested_loop_join(customer_data, 0, orders_data, 0)
    #joined_data = hash_join(customer_data, 0, orders_data, 0)
    joined_data = sort_merge_join(customer_data, orders_data)

    for row in joined_data:
        print(row)

    print("tuples:" , len(joined_data))

def join_on_sorted():
    # join customer's first column with orders' first column, could be a nested loop join, hash join, sort merge join etc.
    
    #joined_data = nested_loop_join(customer_data, 0, orders_data, 0)
    joined_data = hash_join_singlecol(customer_data, 0, orders_data, 0)
    #joined_data = sort_merge_join(customer_data, orders_data)

    for row in joined_data:
        print(row)

    print("tuples:" , len(joined_data))

# nested loop join function
def nested_loop_join(table1, key1, table2, key2):
    result = []
    for row1 in table1:
        for row2 in table2:
            # if key1 of table1 matches key2 of table2, join rows and add to result
            if row1[key1] == row2[key2]:
                result.append(row1 + row2)

    return result

def hash_join(table1, index1, table2, index2):
    dict = {}

    # build phase
    for row1 in table1:
        value1 = dict.get(row1[index1])
        if value1 is None:
            dict[row1[index1]] = 1
        else:
            dict[row1[index1]] = value1 + 1

    result = []

    # probe phase
    for row2 in table2:
        value2 = dict.get(row2[index2])
        if value2 is not None:
            for x in range(value2):
                result.append(row2 + row2)

    return result

def hash_join_singlecol(table1, index1, table2, index2):
    dict = {}

    # build phase
    for row1 in table1:
        value1 = dict.get(row1)
        if value1 is None:
            dict[row1] = 1
        else:
            dict[row1] = value1 + 1

    result = []

    # probe phase
    for row2 in table2:
        value2 = dict.get(row2)
        if value2 is not None:
            for x in range(value2):
                result.append(row2 + row2)

    return result

def sort_merge_join(table1, table2):
    result = []

    table1.sort()
    table2.sort()

    length1 = len(table1)
    length2 = len(table2)

    index1 = 0
    index2 = 0

    t2_at_end = False
    t1_at_end = False

    while not t1_at_end or not t2_at_end:
        if not t2_at_end and table1[index1] > table2[index2]:
            index2 += 1
        if t2_at_end or table1[index1] < table2[index2]:
            index1 += 1
            if index1 >= length1:
                t1_at_end = True
            if index1 < length1 and index2 - 1 >= 0 and table1[index1] == table2[index2 - 1]:
                while(index2 - 1 >= 0 and table2[index2 - 1] == table1[index1]):
                    index2 = index2 - 1
        if index1 < length1 and index2 < length2 and table1[index1] == table2[index2]:
            result.append(table1[index1] + table2[index2])
            index2 += 1
            if index2 >= length2:
                t2_at_end = True

    return result

def insert_test_case(left_data, right_data):
    clear_data = "delete from customer; delete from orders;"
    insert_query_customer = "INSERT INTO customer values " + ",".join(map(lambda x: "(" + str(x)  + ")", left_data)) +";"
    insert_query_orders = "INSERT INTO orders values " + ",".join(map(lambda x: "(" + str(x)  + ")", right_data)) +";"
    conn = sqlite3.connect(db_filename)
    conn.executescript(clear_data)
    conn.executescript(insert_query_customer)
    conn.executescript(insert_query_orders)

def test_join():
    insert_test_case([1,2,3], [4,5,6])
    # sanity check if the join implementation is correct or not, here it just check the # of tuples returned match, c
    # change the join implementation to the one u want
    assert len(join_all_on_sqllite()) == 0

    insert_test_case([1,2,3], [1,2,3])
    assert len(join_all_on_sqllite()) == 3

    insert_test_case([3,2,1], [1,2,3])
    assert len(join_all_on_sqllite()) == 3

    insert_test_case([1,1,3], [1,2,3])
    assert len(join_all_on_sqllite()) == 3

    insert_test_case([1,1,1], [1,1,3])
    assert len(join_all_on_sqllite()) == 6

    insert_test_case([1,3,2], [1,1,3])
    assert len(join_all_on_sqllite()) == 3

    insert_test_case([1,5,7,11,13], [2,5,5,8,8,11,11,13,13])
    assert len(join_all_on_sqllite()) == 6
    
#create_db()
#load_data(20000)
#join_all_on_sqllite()
#join_on_mini_trino()
join_on_sorted()
