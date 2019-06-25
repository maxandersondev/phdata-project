from json import dumps
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

#line = '209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"'

f = open("accessLogs.txt", "r")

for line in f:
    print(line)
    producer.send('phdata', value=line)
    producer.flush()

