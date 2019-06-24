from kafka import KafkaConsumer
import datawriter

consumer = KafkaConsumer(
    'phdata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group')


myWriter = datawriter.DataWriter()
for message in consumer:
    message = message.value.decode('utf-8')[1:]  #get rid of double quote at front

    splitMessage = message.split(' ', 1)
    print("got a message: " + message)
    print("inserting ip: '" + splitMessage[0] + "' log: '" + splitMessage[1] + "'")
    myWriter.insertRecord(message.split(' ', 1))

