#!/bin/sh

file_path="/tmp/clusterID/cluster_id"
interval=5  # wait interval in seconds

while [ ! -e "$file_path" ] || [ ! -s "$file_path" ]; do
  echo "Waiting for $file_path to be created..."
  sleep $interval
done

cluster_id=$(cat "$file_path")

# KRaft required step: Format the storage directory with a new cluster ID
echo "Use $1.properties as kraft config file"

cd /opt/kafka/

bin/kafka-storage.sh format -t $cluster_id -c /opt/kafka/config/kraft/$1.properties
bin/kafka-server-start.sh config/kraft/$1.properties