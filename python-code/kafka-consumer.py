from kafka import KafkaConsumer
from json import loads
import datawriter

consumer = KafkaConsumer(
    'phdata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='spark-streaming',
     value_deserializer=lambda m: loads(m.decode('utf-8')))


myWriter = datawriter.DataWriter()
for message in consumer:
    myWriter.insertRecord(message.value)

