#!/bin/sh

file_path="/tmp/clusterID/cluster_id"
interval=5  # wait interval in seconds

while [ ! -e "$file_path" ] || [ ! -s "$file_path" ]; do
  echo "Waiting for $file_path to be created..."
  sleep $interval
done

cluster_id=$(cat "$file_path")

# KRaft required step: Format the storage directory with a new cluster ID
echo "Use kraft/server.properties as kraft config file"

cd /opt/kafka/

echo "Starting Kafka using KRaft mode..."
bin/kafka-storage.sh format -t $cluster_id -c /opt/kafka/config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties &


# 检查kafka是否启动
max_wait=10  # 设置最大等待次数为60次
counter=0    # 初始化计数器
while true; do
  if netstat -tuln | grep ":9092"; then
    echo "Kafka has started successfully and is listening on port 9092."
    break
  elif [ $counter -ge $max_wait ]; then
    echo "Kafka failed to start within the maximum wait time."
    exit 1
  else
    echo "Kafka is still starting. Waiting..."
    sleep 2
    counter=$((counter + 1))
  fi
done

sleep 5 # 等待其他机器启动

# 启动connect
echo "Starting connect..."
bin/connect-distributed.sh config/connect-distributed.properties