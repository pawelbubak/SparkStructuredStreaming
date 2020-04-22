#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
bash -c "/usr/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-m:9092 --topic bicycles-output --from-beginning" &
bash -c "/usr/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-m:9092 --topic bicycles-anomaly --from-beginning"