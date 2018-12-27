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
    
    df.printSchema() 

    df.createOrReplaceTempView("weather_data")
    # spark.sql(" SELECT BEGIN_YEARMONTH,STATE,EVENT_TYPE FROM weather_data WHERE EVENT_TYPE = 'Winter Storm'").show()
    # spark.sql(" SELECT COUNT(EVENT_ID),BEGIN_YEARMONTH FROM weather_data GROUP BY BEGIN_YEARMONTH ORDER BY BEGIN_YEARMONTH ASC").show()
    df_upit = spark.sql(" SELECT YEAR,COUNT(EVENT_ID) FROM weather_data GROUP BY YEAR ORDER BY YEAR ASC")
    df_upit.show()
    df_upit.repartition(1).write \
    .option("header", "true") \
    .mode('overwrite') \
    .csv('hdfs://namenode:8020/weather/results/number_of_storm_per_year')
    
#   Upiti gde se steta meri u milionima
    query = """ SELECT YEAR,COUNT(EVENT_ID) FROM weather_data 
    WHERE DAMAGE_PROPERTY LIKE '%M' OR DAMAGE_CROPS LIKE '%M' 
    GROUP BY YEAR ORDER BY YEAR ASC"""
    df_upit = spark.sql(query) 
    df_upit.show()
    df_upit.repartition(1).write \
    .option("header", "true") \
    .mode('overwrite') \
    .csv('hdfs://namenode:8020/weather/results/number_of_storm_per_year_with_great_demage')