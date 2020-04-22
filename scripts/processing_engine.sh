#!/bin/sh

P=${1}
D=${2}

echo "Preparing data processing engine"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

echo "Configuring kafka"
echo "Delete old topics"
/usr/local/kafka/bin/kafka-topics.sh --delete \
 --zookeeper ${CLUSTER_NAME}-m:2181 \
 --topic bicycles-output
/usr/local/kafka/bin/kafka-topics.sh --delete \
 --zookeeper ${CLUSTER_NAME}-m:2181 \
 --topic bicycles-anomaly

echo "Create kafka topic: bicycles-output"
/usr/local/kafka/bin/kafka-topics.sh --create \
 --zookeeper ${CLUSTER_NAME}-m:2181 \
 --replication-factor 1 --partitions 1 --topic bicycles-output

echo "Create kafka topic: bicycles-anomaly"
/usr/local/kafka/bin/kafka-topics.sh --create \
 --zookeeper ${CLUSTER_NAME}-m:2181 \
 --replication-factor 1 --partitions 1 --topic bicycles-anomaly

echo "Kafka configuration completed."

spark-submit --class com.bigdata.spark.SparkApplication spark.jar ${P} ${D} --master local[2]

