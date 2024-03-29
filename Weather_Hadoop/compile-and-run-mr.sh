DIR=/hadoop_weather

cd $DIR

#printf "\nCOPY FILE TO HDFS\n"
#$HADOOP_PREFIX/bin/hdfs dfs -put *.csv /weather

printf "\nSETTING EXECUTEABLE PY\n"

chmod a+x *.py

cd $HADOOP_PREFIX

printf "\nRUN HADOOP-STREAMING\n"

bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar \
    -input /weather \
    -output /hadoop_weather_results \
    -mapper $DIR/mapper.py \
    -reducer $DIR/reducer.py

printf "\nRESULTS\n"

$HADOOP_PREFIX/bin/hdfs dfs -cat /hadoop_weather_results/*
