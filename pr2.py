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
        .csv("hdfs://namenode:8020/weather/*_red.csv")
    df.createOrReplaceTempView("weather_data")
        
#   Upiti gde se steta meri u milionima
    query = """ SELECT YEAR,COUNT(EVENT_ID) FROM weather_data 
    WHERE DAMAGE_PROPERTY LIKE '%M' OR DAMAGE_CROPS LIKE '%M' 
    GROUP BY YEAR ORDER BY YEAR ASC"""
    df_upit = spark.sql(query) 
    df_upit.show()
    df_upit.repartition(1).write.mode('overwrite').csv('hdfs://namenode:8020/weather/results/number_of_storm_per_year_with_great_demage')