import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re

#/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 /opt/extra1/Development/code/git/phdata-project/python-code/ip-attack-spark.py

def sendRecord(record):
    file = open("/tmp/ipAttack.txt", "a")
    file.write(record)
    file.flush()

def createContext():
    """
    createContect()
    This will do the work of filtering the stream coming from kafka, it will sort and filter
    :return: SSC
    """
    BATCH_TIME_FRAME_IN_SECONDS: int = 60
    SLIDE_WINDOW_IN_SECONDS: int = 60
    ATTACK_THRESHOLD: int = 2
    sc = SparkContext(appName="phDataIPAttackProject")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, BATCH_TIME_FRAME_IN_SECONDS)

    kvs = KafkaUtils.createDirectStream(ssc, ["phdata"], {"metadata.broker.list": "localhost:9092"})

    # Count number of tweets in the batch
    count_this_batch = kvs.count().map(lambda x: ('Tweets this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kvs.countByWindow(BATCH_TIME_FRAME_IN_SECONDS, SLIDE_WINDOW_IN_SECONDS).map(lambda x: ('Hits total (%s second rolling count): %s' % (str(BATCH_TIME_FRAME_IN_SECONDS), x)))

    regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "-" "(.*?)"'

    #get ips
    ip_dstream = kvs.map(lambda x: re.match(regex, x[1][1:-1].encode('utf-8').decode('unicode_escape')).groups()[0])

    # Count each value and number of occurences in the batch windowed
    count_values_windowed_dstream = ip_dstream.countByValueAndWindow(BATCH_TIME_FRAME_IN_SECONDS, SLIDE_WINDOW_IN_SECONDS).transform(lambda y: y.sortBy(lambda x: -x[1]))
    # Filtered to ATTACK_THRESHOLD
    count_values_windowed_dstream_filtered = count_values_windowed_dstream.filter(lambda x: x[1] > ATTACK_THRESHOLD)
    # Make the output a bit easier to read
    count_values_windowed_dstream_filtered_pretty = count_values_windowed_dstream_filtered.map(lambda x: "IP: %s \t count: %s\n" % (x[0], x[1]))
    #keep the counts grouped in the output
    count_this_batch.union(count_windowed).pprint()
    # Print to screen
    count_values_windowed_dstream_filtered_pretty.pprint()
    #iterate and send results to text file
    count_values_windowed_dstream_filtered_pretty.foreachRDD(lambda x: x.foreach(sendRecord))

    return ssc


if __name__ == "__main__":

    ssc = StreamingContext.getOrCreate('/tmp/ipAttack_v023.txt', lambda: createContext())
    ssc.start()
    ssc.awaitTermination()

