from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, from_unixtime, unix_timestamp
import re

sc = SparkContext(appName="spark-twitter")

ssc = StreamingContext(sc, 5)

ss = SparkSession.builder.appName("spark-twitter")           	\
 	.config("spark.sql.warehouse.dir", "/user/hive/warehouse")  \
 	.config("hive.metastore.uris", "thrift://localhost:9083")   \
 	.enableHiveSupport().getOrCreate()

ss.sparkContext.setLogLevel('WARN')

# save the dataframe to mysql database
def exportToMySQL(df):
	print("********** Exporting to database table **********")
	df.show()
	mysql_url = "jdbc:mysql://192.168.56.101:3306/bigdata2020?user=test4&password=Abcd1234!"
	df.write.jdbc(mysql_url, table="tweets", mode="append")

# convert rdd to dataframe and retrieve interested columns
def transform(rdd):
	print("********** Start processing rdd **********")
	if not rdd.isEmpty():
    	global ss
    	df = ss.read.json(rdd)
    	df_selected = df.select("id", "source", "favorited", "text",  \
                        	from_unixtime(unix_timestamp("created_at", "EEE MMM dd HH:mm:ss ZZZZ yyyy")).alias("created_at"), \
                        	col("user.screen_name").alias("user_screen_name"),     	\
                        	col("user.name").alias("user_name"),                   	\
                        	col("user.friends_count").alias("user_friends_count"), 	\
                        	col("user.followers_count").alias("user_followers_count"), \
                        	"in_reply_to_screen_name")
    	exportToMySQL(df_selected)

# streaming the twitter data from hdfs
print("********** Start a new streaming batch **********")
 
lines = ssc.textFileStream("hdfs://master:9000/flume_twitter")

lines.map(lambda l: l.lower())          	\
 	.filter(lambda l: l.replace('#', ''))   	\
 	.filter(lambda l: re.sub(r'[^\x00-\x7F]+', ' ', l))  \
 	.foreachRDD(lambda l: transform(l))

ssc.start()
ssc.awaitTermination()

