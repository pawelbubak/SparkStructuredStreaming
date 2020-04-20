#!/bin/sh

echo "Preparing data processing engine"

spark-submit --class com.bigdata.spark.SparkApplication --master local[2]
./spark.jar
