from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, col, from_unixtime, unix_timestamp

sc = SparkContext(appName="twitter_flume-spark-mysql")

ss = SparkSession.builder.appName("twitter_flume-spark-mysql").getOrCreate()

ss.sparkContext.setLogLevel('WARN')

twitterData = ss.read.json('hdfs://master:9000/flume_twitter')

df_selected = twitterData.select("id", "source", "favorited", "text",  \
                        	from_unixtime(unix_timestamp("created_at", "EEE MMM dd HH:mm:ss ZZZZ yyyy")).alias("created_at"), \
                        	col("user.screen_name").alias("user_screen_name"),     	\
                        	col("user.name").alias("user_name"),                   	\
                        	col("user.friends_count").alias("user_friends_count"), 	\
                        	col("user.followers_count").alias("user_followers_count"), \
                        	"in_reply_to_screen_name")

mysql_url = "jdbc:mysql://192.168.56.101:3306/bigdata2020?user=[user_name]&password=[password]"

df_selected.write.jdbc(mysql_url, table="flumeTwitter", mode="append")
