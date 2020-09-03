from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint

sc = SparkContext(appName="kafka-spark")
ssc = StreamingContext(sc,5)

ks = KafkaUtils.createDirectStream(ssc, ['bdworld'], {'metadata.broker.list' : 'localhost:9093'})

api_data = ks.map(lambda x: x[1])

api_data.pprint()

ssc.start()
ssc.awaitTermination()
