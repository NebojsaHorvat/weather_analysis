from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def mapByUSState(line):
    columns = line.split(",")
    try:
        injuries = int(columns[20]) + int(columns[21])
    except:
        injuries = 0
    try:
        deaths = int(columns[22]) + int(columns[23])
    except:
        deaths = 0
    try:
        matherial_demage = float(columns[24][:-1])
        matherial_demage_scal = columns[24][-1]
        if matherial_demage_scal == 'K' :
            matherial_demage *= 1000
        elif matherial_demage_scal == 'M' :
            matherial_demage *= 1000000
    except:
        matherial_demage = 0.0
    try:
        state = columns[8]
    except:
        state = "ERROR"

    return (state , ( injuries, deaths, matherial_demage) )


def reduceUnifiedDisaster(a,b):
    return [ (a[0] + b[0])*1000000, (a[1] + b[1])*1000, a[2] + b[2]]
        
if __name__ == "__main__":
    
    conf = SparkConf().setAppName("pr1").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("hdfs://namenode:8020/weather/*_red.csv")

    pairs = lines.map(mapByUSState)

    states = pairs.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1], a[2] + b[2]])
    number = states.count()
    statesCollected = states.collect()
    print "EVERY STATE HAS:" 
    for state in statesCollected:
        print state ,'\n'
    print "NUMBER OF STATESL", number
    states.saveAsTextFile("hdfs://namenode:8020/weather/results/disaster_by_state")

#   Pokusaj odredjivanja drzave koja je pretrpela najvise nepogoda
    unifiedDisasteByState = []
    for state in statesCollected:
        unifiedDisaster = state[1][0]*1000000 + state[1][1]*1000 + state[1][2]
        unifiedDisasteByState += [(state[0],unifiedDisaster)]

    print "UNIFIED DISASTER OF STATE :" 
    for state in unifiedDisasteByState:
        print state ,'\n'

    maxState = unifiedDisasteByState[0]
    for state in unifiedDisasteByState:
        if state[1] > maxState[1]:
            maxState = state
    

    sparkSession = SparkSession.builder.appName("weather_app").getOrCreate()
    df = sparkSession.createDataFrame(unifiedDisasteByState)
    df.write.mode('overwrite').csv("hdfs://namenode:8020/weather/results/unified_disaster_by_state")

    df = sparkSession.createDataFrame([maxState])
    df.write.mode('overwrite').csv("hdfs://namenode:8020/weather/results/max_unified_disaster_by_state")
    # f= open("hdfs://namenode:8020/weather/results/disaster_by_state.txt","w+")
    # for state in unifiedDisasteByState:
    #     f.write(state)
    # f.write( "\nState which suffered most from elements: \n")
    # f.write(maxState)
    # f.close()
    