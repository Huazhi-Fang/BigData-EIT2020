from pyspark import SparkContext

if __name__ == "__main__":

  # create Spark context with Spark configuration
  sc = SparkContext(appName="Text_to_RDD")

  # read input text file to RDD
  text_RDD = sc.textFile("./Shakespeare.txt")

  # collect the RDD into a list
  text_list = text_RDD.collect()

  # print the list
  for line in text_list:
	  print(line)
