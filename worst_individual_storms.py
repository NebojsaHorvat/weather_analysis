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


    query = """ SELECT YEAR, STATE,EVENT_TYPE ,DAMAGE_PROPERTY ,DAMAGE_CROPS, DEATHS_DIRECT, INJURIES_DIRECT
    FROM weather_data DEATHS_DIRECT
    WHERE DAMAGE_PROPERTY LIKE '%M' AND DAMAGE_CROPS LIKE '%M' AND DEATHS_DIRECT > 10 AND INJURIES_DIRECT > 10
    ORDER BY DEATHS_DIRECT DESC"""
    df_upit = spark.sql(query) 
    df_upit.show()

    df_upit.repartition(1).write \
    .option("header", "true") \
    .mode('overwrite') \
    .csv('hdfs://namenode:8020/weather/results/worst_individual_disasters')