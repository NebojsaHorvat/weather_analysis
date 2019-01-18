from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def reduceUnifiedDisaster(a,b):
    return [ (a[0] + b[0])*1000000, (a[1] + b[1])*1000, a[2] + b[2]]
        
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("weater_app") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv("hdfs://namenode:8020/weather/*red.csv")
        # .option("mode", "DROPMALFORMED") \
    # df.select("STATE").show()
    
    df.createOrReplaceTempView("weather_data")


    query = """ SELECT MONTH_NAME,EVENT_TYPE, count(EVENT_ID)
    FROM weather_data 
    GROUP BY MONTH_NAME,EVENT_TYPE
    HAVING COUNT(EVENT_ID) > 3
    ORDER BY MONTH_NAME ASC"""
    df_upit = spark.sql(query) 
    df_upit.show()
    

    df_upit.repartition(1).write \
    .option("header", "true") \
    .mode('overwrite') \
    .csv('hdfs://namenode:8020/weather/results/disaster_type_count_by_month')

    
