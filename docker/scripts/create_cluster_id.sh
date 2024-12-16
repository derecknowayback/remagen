#!/bin/bash

cluster_id=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
echo $cluster_id > /tmp/clusterID/cluster_id
echo "Cluster id [$cluster_id] has been  created..."
