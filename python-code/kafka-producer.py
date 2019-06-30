from json import dumps
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

f = open("accessLogs.txt", "r")

for line in f:
    print(line)
    producer.send('phdata', value=line)
    producer.flush()

