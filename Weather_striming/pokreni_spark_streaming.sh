#!/bin/bash

chmod +x add-jar.sh 
./add-jar.sh
$SPARK_HOME/bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar ./too_much_storms.py

