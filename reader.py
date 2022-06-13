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
        data = (msg.value['meta']['id'], msg.value['meta']['domain'], msg.value['meta']['creation_time'],
                msg.value['meta']['user_id'], msg.value['meta']['user_name'], msg.value['meta']['page'],
                msg.value['meta']['is_bot'])
        client.insert_category_a(data)
