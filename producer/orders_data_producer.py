import json
import time
import random
from google.cloud import pubsub_v1
import mysql.connector

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "big-data-projects-411817"
topic_name = "orders_data"
topic_path = publisher.topic_path(project_id, topic_name)

# Connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="your_mysql_user",
    password="your_mysql_password",
    database="ecom_db"
)
cursor = db_conn.cursor(dictionary=True)

def fetch_order_data(order_id):
    cursor.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
    return cursor.fetchone()

def callback(future):
    try:
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

order_id = 1
while True:
    data = fetch_order_data(order_id)
    if data:
        json_data = json.dumps(data).encode('utf-8')
        try:
            future = publisher.publish(topic_path, data=json_data)
            future.add_done_callback(callback)
            future.result()
        except Exception as e:
            print(f"Exception encountered: {e}")

        time.sleep(2)

    order_id += 1
    if order_id > 80:
        order_id = 1
