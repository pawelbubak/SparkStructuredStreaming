#!/bin/sh

echo "Preparing project workspace"
rm -rf project-data
mkdir project-data && cd project-data || exit

echo "Downloading data"
mkdir stations && cd stations || exit
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/Divvy_Bicycle_Stations.csv
cd ..
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/bicycle_result.zip
echo "Data download completed"

echo "Unpacking data"
unzip bicycle_result.zip
rm bicycle_result.zip
echo "Unpacking data completed"

cd ..

echo "Configuring kafka"
/usr/lib/kafka/bin/kafka-topics.sh --delete \
 --zookeeper localhost:2181 \
 --topic testTopic
/usr/lib/kafka/bin/kafka-topics.sh --create \
 --zookeeper localhost:2181 \
 --replication-factor 1 --partitions 1 --topic kafka-tt-bicycles-input
echo "Kafka configuration completed. Created kafka topic [kafka-tt-bicycles-input]"
echo "Workspace preparation completed"

echo "Runing producer"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer project-data/bicycle_result 2 kafka-tt-bicycles-input \
 0 ${CLUSTER_NAME}-w-0:9092
