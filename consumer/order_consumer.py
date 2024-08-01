import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Subscription details
project_id = "Real-Time-Data-Proc-Pipeline"
subscription_name = "orders_data-sub"
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def cassandra_connection():
    CASSANDRA_NODES = ['127.0.0.1']
    CASSANDRA_PORT = 9042
    KEYSPACE = 'ecom_store'
    
    auth_provider = PlainTextAuthProvider(username='admin', password='admin')
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect(KEYSPACE)

    return cluster, session

# Setup Cassandra connection
cluster, session = cassandra_connection()

# Prepare the Cassandra insertion statement
insert_stmt = session.prepare("""
    INSERT INTO orders_payments_facts (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date, payment_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Pull and process messages
def pull_messages():
    while True:
        response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 10})
        ack_ids = []

        for received_message in response.received_messages:
            json_data = received_message.message.data.decode('utf-8')
            deserialized_data = json.loads(json_data)

            cassandra_data = (
                deserialized_data.get("order_id"),
                deserialized_data.get("customer_id"),
                deserialized_data.get("item"),
                deserialized_data.get("quantity"),
                deserialized_data.get("price"),
                deserialized_data.get("shipping_address"),
                deserialized_data.get("order_status"),
                deserialized_data.get("creation_date"),
                None,
                None,
                None,
                None,
                None
            )
            
            session.execute(insert_stmt, cassandra_data)
            print("Data inserted in Cassandra -> ", deserialized_data)
            ack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

if __name__ == "__main__":
    try:
        pull_messages()
    except KeyboardInterrupt:
        pass
    finally:
        cluster.shutdown()
