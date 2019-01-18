
# Big data weather analysis

Project is focused on analysing big amounts of weather data and using them to extract information.
Project contains 3 elements:
- Bach processing using map/reduce with Hadoop 
- Bach processing using spark RDD and Spark SQL
- Stream processing using Spark straming 

ALL INSTRUCTIONS REQUIRE DATA TO BE ON HDFS IN /weather DIR !!
## Starting  Hadoop batch processing
- Go to ./Docker/Docker-Hadoop
- start docker containers with docker-compose up command 
- copy file witch on spark-master container ( docker cp . spark-master:/spark)
- connect with spark-master container (docker exec -it spark-master /bin/bash/)
- run example with command : $SPARK_HOME/bin/spark-submit ./spark/disaster_type_count_by_month.py

## Starting  Spark batch processing
- Go to ./Docker/Docker-spark
- start docker containers with docker-compose up command 
- position in Weather/Weather_Hadoop and run ./copy-files.sh
- connect with name-node container (docker exec -it docker-hadoop_namenode_1 /bin/bash/)
- run example with script ./compile-and-run-mr.sh

## Starting  Spark streaming
- Go to ./Docker/Docker-spark-streaming
- start docker containers with docker-compose up command 
- position in Weather/Weather_streaming and run ./kopiraj_fajlove_u_spark_master.sh
- connect with name-node container (docker exec -it docker-hadoop_namenode_1 /bin/bash/)
- run ./add-jars.sh to install kafka dependencies for spark 
- run example with command : $SPARK_HOME/bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar ./too_much_storms.py


