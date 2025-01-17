package com.dereckchen.remagen.demo;

import com.dereckchen.remagen.kafka.interceptor.KafkaBridgeConsumer;
import com.dereckchen.remagen.models.BridgeOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.*;

@Slf4j
public class KafkaSubscribe {
    public static void main(String[] args) {
        // Kafka broker地址
        String bootstrapServers = "localhost:39092,localhost:39093,localhost:39094";
        // 主题名称
        String topic = "new_top";
        String mqttTopic = "mqtt_topic_test12";
        // 消费者组ID
        String groupId = "my-group";

        // 配置消费者
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // 从最早的消息开始消费

        // 创建消费者实例
        String host = "localhost", port = "38083";
        KafkaBridgeConsumer<String,String> consumer = new KafkaBridgeConsumer<>(props,host,port,false);

        // 订阅主题
        BridgeOption bridgeOption = new BridgeOption();
        bridgeOption.setKafkaTopic(topic);
        bridgeOption.setMqttTopic(mqttTopic);
        bridgeOption.setProps(getBrideProps(topic,mqttTopic));
        consumer.subscribe(Collections.singletonList(topic),bridgeOption);

        // 消费消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received message: key = {}, value = {}, offset = {}, partition = {}", record.key(), record.value(), record.offset(), record.partition());
                }
            }
        } finally {
            // 关闭消费者
            consumer.close();
        }
    }

    static Map<String, String> getBrideProps(String topic, String mqttTopic) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.source.MqttSourceConnector");
        props.put("tasks.max", "1");
        props.put("topic", topic);
        props.put("mqtt.topic", mqttTopic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.username", "admin");
        props.put("mqtt.password", "public");
        props.put("mqtt.clientid", "client-i783e3d-1234");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        return props;
    }
}
