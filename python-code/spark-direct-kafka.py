import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re

#/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 /opt/extra1/Development/code/git/phdata-project/python-code/spark-direct-kafka.py

def process_row(message):
    regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "-" "(.*?)"'
    logList = re.match(regex, message).groups()
    return lambda x, y: re.match(x, y).groups()

if __name__ == "__main__":
    TIME_FRAME_IN_SECONDS = 60
    ATTACK_THRESHOLD = 20
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, TIME_FRAME_IN_SECONDS)
    #brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, ["phdata"],{"metadata.broker.list": "localhost:9092"})
    regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "-" "(.*?)"'


    ip_dstream = kvs.map(lambda x: re.match(regex, x[1][1:-1].encode('utf-8').decode('unicode_escape')).groups()[0])

    ip_counts = ip_dstream.countByValue()
    #ip_counts.pprint()

    ip_counts_sorted_dstream = ip_counts.transform(lambda y: y.sortBy(lambda x:( -x[1])))

    #ip_counts_sorted_dstream.pprint()
    filtered_ips = ip_counts.filter(lambda x: x[1] > ATTACK_THRESHOLD)
    filtered_ips_dstream = filtered_ips.transform(lambda y: y.sortBy(lambda x:( -x[1])))
    filtered_ips_dstream.pprint()
    #lines = kvs.map(lambda x: x[1][1:-1].encode('utf-8').decode('unicode_escape'))
    #lines.pprint()
    # lines.pprint()
    # lines.count().map(lambda x:'Records in this batch: %s' % x).pprint()




    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #               .map(lambda word: (word, 1)) \
    #               .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
    ssc.awaitTermination()

