#!/bin/bash

# 获取所有正在运行的容器的 ID 和名称
containers=$(docker ps --format "{{.ID}}\t{{.Names}}")

# 遍历每个容器，获取其 IP 地址并打印“容器名-ip”
echo "ContainerName  IP"
while read -r container; do
    id=$(echo $container | awk '{print $1}')
    name=$(echo $container | awk '{print $2}')
    ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $id)
    echo "$name  $ip"
done <<< "$containers"
