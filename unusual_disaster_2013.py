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
        .csv("hdfs://namenode:8020/weather/Stormdata_2013_red.csv")
    df.createOrReplaceTempView("weather_data")
        

    df = spark.read \
        .option("header", "true") \
        .csv("hdfs://namenode:8020/weather/results/disaster_type_count_by_month.csv")
    df.createOrReplaceTempView("disaste_type_month")

#   Upiti gde se steta meri u milionima
    query = """ SELECT STATE,MONTH_NAME, EVENT_TYPE FROM weather_data wd
    WHERE  NOT EXISTS 
    (SELECT MONTH_NAME, STATE,EVENT_TYPE
    FROM   disaste_type_month dtm
    WHERE  wd.MONTH_NAME = dtm.MONTH_NAME AND wd.STATE = dtm.STATE AND wd.EVENT_TYPE = dtm.EVENT_TYPE)
    GROUP BY MONTH_NAME, STATE,EVENT_TYPE
    ORDER BY STATE ASC """
    df_upit = spark.sql(query) 
    df_upit.show(100)
    df_upit.repartition(1).write.mode('overwrite').csv('hdfs://namenode:8020/weather/results/unusual_disasters_2013')