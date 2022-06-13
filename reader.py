from json import loads
from cassandra_client import CassandraClient
from kafka import KafkaConsumer

while True:
    consumer = KafkaConsumer('wikipedia-stream',
                             bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda x: loads(x.decode('ascii')))

    client = CassandraClient()
    client.connect()

    last_hour = None
    added = 0
    for msg in consumer:
        a_data = (msg.value['meta']['id'], msg.value['meta']['domain'], msg.value['meta']['dt'],
                  msg.value['performer']['user_id'], msg.value['meta']['user_text'],
                  msg.value['page_title'], msg.value['meta']['user_is_bot'])
        client.insert_category_a(a_data)

        page_data = (msg.value['page_id'], msg.value['page_title'])
        client.insert_page_info(page_data)
