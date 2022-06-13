from json import dumps, loads
from kafka import KafkaProducer
import requests


producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                         value_serializer=lambda x: dumps(x).encode('ascii'))
url = 'https://stream.wikimedia.org/v2/stream/page-create'

session = requests.Session()

with session.get(url, stream=True) as response:
    for line in response.iter_lines():
        if line:
            message = line.decode('utf8')
            if message[:4] == "data":
                producer.send('wikipedia-stream', loads(message[6:]))

    producer.flush()


