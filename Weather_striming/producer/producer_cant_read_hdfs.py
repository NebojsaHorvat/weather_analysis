import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
import kafka.errors
import pydoop.hdfs as hd
try:
    import thread
except ImportError:
    import _thread as thread

TOPIC = "weather_stream"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

if __name__ == "__main__":
    
    for year in range(1996, 2014): 
        loadString = "hdfs://namenode:8020/weather/Stormdata_" + str(year) + "_red.csv"
        data = pd.read_csv(loadString,low_memory=False)
        for month in range(1, 13):
            for day in range(1,31):
                current_ym = year*100 + month
                beginning_df = data.loc[(data['BEGIN_YEARMONTH'] == current_ym) & (data['BEGIN_DAY'] == day)]
                beginning_df = beginning_df.drop_duplicates(subset='EPISODE_ID')
                ending_df = data.loc[(data['END_YEARMONTH'] == current_ym) & (data['END_DAY'] == day)]
                ending_df = ending_df.drop_duplicates(subset='EPISODE_ID')
                for index, row in beginning_df.iterrows():
                    #print row['BEGIN_YEARMONTH'],row['BEGIN_DAY'],row['STATE'], row['EVENT_TYPE'],row['EPISODE_ID']
                    row['beginning']='True'
                    json_row = row.to_json()
                    # print json_row, "=================================="
                    producer.send(TOPIC, value=json_row, key= row['EPISODE_ID'] )
                    
                for index, row in ending_df.iterrows():
                    row['beginning']='False'
                    json_row = row.to_json()
                    producer.send(TOPIC, value=json_row, key= row['EPISODE_ID'] )
                time.sleep(100) 

