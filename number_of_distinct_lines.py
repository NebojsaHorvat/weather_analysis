from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pr1").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile("hdfs://namenode:8020/weather/Stormdata_1996.csv")
# lines = spark.read.csv(
#     "some_input_file.csv", header=True, mode="DROPMALFORMED", schema=schema
# )
pairs = lines.map(lambda s: (s, 1))
# print "======================="
# print pairs[0]
# print "======================="
# print pairs[1]
# print "======================="
# print pairs[2]
# print "======================="
counts = pairs.reduceByKey(lambda a, b: a + b)
number = counts.count()
print "TOTAL NUMBER OF LINES IN Stormdata_1996_red.csv : " , number
