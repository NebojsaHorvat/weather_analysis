from __future__ import print_function

import sys
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


TOPIC = "weather_stream"

def mapStorms(json_value):
    eventId = json_value["EPISODE_ID"]
    beginning = json_value["BEGINNING"]
    return (eventId,beginning)
    
def reduceStormsByKey(beginning1,beginning2):
    if ((beginning1 == "False") or (beginning1 == "False")):
        return "False"
    return "True"

def filterStorms(element):
    # print ("==========element==================")
    # print (element)
    if element[1] == "True":
        return True
    else :
        return False

if __name__ == "__main__":
    sc = SparkContext(appName="too_much_storms")
	
	# Suppress the output logs
    sc.setLogLevel("ERROR")
	
    ssc = StreamingContext(sc, 3)
    
    directKafkaStream = KafkaUtils.createDirectStream(
        ssc, [TOPIC], {"metadata.broker.list": "kafka:9092"})

    ssc.checkpoint("checkpoint")

    print ("===== spark-streaming-gets =====" )
    number_of_storms = directKafkaStream.map(lambda msg: json.loads(msg[1]))\
    .map(mapStorms)\
    .reduceByKey(reduceStormsByKey)\
    .filter(filterStorms)

    activeStormCount = number_of_storms.count().pprint()
    # print ("Number of active storms: ",activeStormCount )
    
    # number_of_storms.pprint()

    ssc.start()
    ssc.awaitTermination()