import mysql.connector
import random

# Connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="your_mysql_user",
    password="your_mysql_password",
    database="ecom_db"
)
cursor = db_conn.cursor()

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    item TEXT,
    quantity BIGINT,
    price DOUBLE,
    shipping_address TEXT,
    order_status TEXT,
    creation_date TEXT
)""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    payment_method TEXT,
    card_last_four TEXT,
    payment_status TEXT,
    payment_datetime TEXT
)""")

# Insert mock data into orders table
for order_id in range(1, 81):
    customer_id = random.randint(1000, 2000)
    item = random.choice(['Item A', 'Item B', 'Item C'])
    quantity = random.randint(1, 10)
    price = round(random.uniform(10.0, 100.0), 2)
    shipping_address = '123 Main St'
    order_status = 'Completed'
    creation_date = f"2024-01-21T{str(order_id).zfill(2)}:01:30Z"

    cursor.execute("""
    INSERT INTO orders (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date))

# Insert mock data into payments table
for order_id in range(1, 81):
    payment_id = order_id + 1000
    payment_method = random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Google Pay', 'Apple Pay'])
    card_last_four = str(order_id).zfill(4)[-4:]
    payment_status = 'Completed'
    payment_datetime = f"2024-01-21T{str(order_id).zfill(2)}:01:30Z"

    cursor.execute("""
    INSERT INTO payments (payment_id, order_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (payment_id, order_id, payment_method, card_last_four, payment_status, payment_datetime))

db_conn.commit()
cursor.close()
db_conn.close()
