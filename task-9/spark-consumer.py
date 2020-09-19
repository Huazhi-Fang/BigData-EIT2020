from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode, col
import json

import pyspark.sql.functions as F
import pyspark.sql.types as T

zip_udf = F.udf(lambda x,y,z: list(zip(x,y,z)),
      T.ArrayType(T.StructType([
          T.StructField("id", T.StringType()),
          T.StructField("name", T.StringType()),
          T.StructField("href", T.StringType())
])))

sc = SparkContext(appName="kafka-spark")
ssc = StreamingContext(sc,10)

ss = SparkSession.builder.getOrCreate()
ss.sparkContext.setLogLevel('WARN')

def transform(rdd):
    if not rdd.isEmpty():
        global ss
        df = ss.read.json(rdd)
        df1 = df.select("items.id", "items.name", "items.href", "items.artists")
        df2 = df1.select(explode(zip_udf("id", "name", "href")).alias("album"), "artists")
        df3 = df2.select("album", explode("artists").alias("artists"))
        df4 = df3.select("album", explode("artists").alias("artists"))

        df5 = df4.select(col("album.id").alias("album_id"), col("album.name").alias("album_name"), \
                         col("album.href").alias("album_href"), col("artists.id").alias("artists_id"), \
                         col("artists.name").alias("artists_name"))
        df5 = df5.distinct()
        df5.show()

ks = KafkaUtils.createDirectStream(ssc, ['bdworld'], {'metadata.broker.list' : 'sandbox-hdp:6667'}, valueDecoder=lambda s: json.loads(s.decode('utf-8')))

lines = ks.map(lambda x: x[1])
lines.pprint()

lines.foreachRDD(lambda rdd: transform(rdd))

ssc.start()
ssc.awaitTermination()
