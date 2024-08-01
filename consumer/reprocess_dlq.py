import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Project and Topic details
project_id = "big-data-projects-411817"
subscription_name = "dlq_payments_data-sub"
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

# Pull and process DLQ messages
def reprocess_dlq():
    while True:
        response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 10})
        ack_ids = []

        for received_message in response.received_messages:
            json_data = received_message.message.data.decode('utf-8')
            deserialized_data = json.loads(json_data)

            query = f"SELECT order_id FROM orders_payments_facts WHERE order_id = {deserialized_data.get('order_id')}"
            rows = session.execute(query)
            if rows.one():
                update_query = """
                    UPDATE orders_payments_facts 
                    SET payment_id = %s, 
                        payment_method = %s, 
                        card_last_four = %s, 
                        payment_status = %s, 
                        payment_datetime = %s 
                    WHERE order_id = %s
                """
                values = (
                    deserialized_data.get('payment_id'),
                    deserialized_data.get('payment_method'),
                    deserialized_data.get('card_last_four'),
                    deserialized_data.get('payment_status'),
                    deserialized_data.get('payment_datetime'),
                    deserialized_data.get('order_id')
                )
                session.execute(update_query, values)
                print("Reprocessed DLQ entry and updated in Cassandra -> ", deserialized_data)

            ack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

if __name__ == "__main__":
    try:
        reprocess_dlq()
    except KeyboardInterrupt:
        pass
    finally:
        cluster.shutdown()
