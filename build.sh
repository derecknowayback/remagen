#!/bin/bash

# 定义模块顺序
MODULES=("remagen-utils" "remagen-kafka-restful-client" "remagen-mqtt-client" "remagen-kafka-interceptor")

# 先构建基础模块
for module in "${MODULES[@]}"
do
    echo "=== 开始构建 $module ==="
    cd $module
    mvn clean install -DskipTests
    if [ $? -ne 0 ]; then
        echo "构建 $module 失败"
        exit 1
    fi
    cd ..
    echo "=== $module 构建完成 ==="
done

# 最后构建 connect 模块
echo "=== 开始构建 connect 模块 ==="
cd remagen-connect
mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo "构建 connect 失败"
    exit 1
fi
cd ..
echo "=== connect 构建完成 ==="

echo "所有模块构建完成！"

echo "拷贝 jar 到 docker libs 目录"
cp remagen-connect/target/remagen-connect-1.0-SNAPSHOT.jar docker/libs