from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint
import json

from pyspark.sql.functions import explode, col
import pyspark.sql.functions as F
import pyspark.sql.types as T

arrays_zip = F.udf(lambda x, y, z: list(zip(x, y, z)),
    T.ArrayType(T.StructType([
         T.StructField("category_id", T.StringType()),
         T.StructField("category_label", T.StringType()),
         T.StructField("products", T.ArrayType(T.StructType([
             T.StructField("description", T.StringType()),
             T.StructField("endDate", T.StringType()),
             T.StructField("id", T.StringType()),
             T.StructField("imageUrl", T.StringType()),
             T.StructField("normalPrice", T.StringType()),
             T.StructField("offerPercent", T.StringType()),
             T.StructField("offerPrice", T.StringType()),
             T.StructField("reviewRating", T.StringType()),
             T.StructField("title", T.StringType()),
             T.StructField("totalReviews", T.StringType()),
             T.StructField("url", T.StringType())
         ])))
     ]))
)

sc = SparkContext(appName="spark-kafka-consumer")
ssc = StreamingContext(sc,10)

ss = SparkSession.builder.appName("spark-kafka-consumer").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083").enableHiveSupport().getOrCreate()
ss.sparkContext.setLogLevel('WARN')

def exportToHive(df):

    df1 = df.select("update_time", "offers.category_id", \
                         "offers.category_label", "offers.products")

    df2 = df1.withColumn("cat_prod", explode(arrays_zip("category_id", "category_label", "products")))

    df3 = df2.select("update_time", \
                      col("cat_prod.category_id").alias("category_id"), \
                      col("cat_prod.category_label").alias("category_label"), \
                      col("cat_prod.products").alias("products"))

    df4 = df3.select("update_time", "category_id", "category_label", \
                     explode("products").alias("products"))

    df5 = df4.select("update_time", "category_id", "category_label", \
                      col("products.id").alias("product_id"), \
                      col("products.title").alias("title"), \
                      col("products.normalPrice").alias("normalPrice"), \
                      col("products.offerPercent").alias("offerPercent"), \
                      col("products.offerPrice").alias("offerPrice"), \
                      col("products.endDate").alias("endDate"), \
                      col("products.reviewRating").alias("reviewRating"), \
                      col("products.totalReviews").alias("totalReviews"), \
                      col("products.description").alias("description"), \
                      col("products.imageUrl").alias("imageUrl"), \
                      col("products.url").alias("url"))

    df5.select("product_id", "title", "normalPrice", "offerPrice", "url").show(10,True)

    df5.write.saveAsTable(name='bigdata2020.rapidAPIdata', format='hive', mode='append')

def transform(rdd):
    if not rdd.isEmpty():
        global ss
        df = ss.read.json(rdd)
        exportToHive(df)

# may need run: export PYTHONIOENCODING=utf8 to avoid the UnicodeEncodeError
ks = KafkaUtils.createDirectStream(ssc, ['bdworld'], {'metadata.broker.list' : 'sandbox-hdp:6667'}, valueDecoder=lambda s: json.loads(s.decode('utf-8')))

api_data = ks.map(lambda x: x[1])
api_data.pprint()

api_data.foreachRDD(lambda rdd: transform(rdd))

ssc.start()
ssc.awaitTermination()
