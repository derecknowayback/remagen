package com.dereckchen.remagen.demo;

import com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class MqttSinkDemo {
    public static void main(String[] args) {
        // Kafka broker地址
        String bootstrapServers = "localhost:9092";
        // 主题名称
        String topic = "kafka_mqtt_sink";
        String mqttTopic = "test_mqtt";


        // 配置生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.dereckchen.remagen.kafka.interceptor.BridgeProducerInterceptor");

        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "8848");
        props.put("kafkaConnectManager.needHttps", "false");


        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);


        String key = "1";
        String value = "{\n" +
                "\"uid\":1,\n" +
                "\"cost\":100\n" +
                "}";

        // 发送消息
        while (true){
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key+Math.random(), value);

            BridgeOption option = new BridgeOption(mqttTopic, topic,getProps(topic, mqttTopic));

            Headers headers = record.headers();
            headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
            headers.add(KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY, JsonUtils.toJsonBytes(option));


            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Message sent successfully: {}" , metadata.toString());
                } else {
                    log.error("Error sending message: {}", exception.getMessage());
                }
            });
        }
    }

    public static Map<String,String> getProps(String topic, String mqttTopic) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class","com.dereckchen.remagen.kafka.connector.sink.MqttSinkConnector");
        props.put("tasks.max","1");
        props.put("topics",topic); // todo 重点观察是传类型还是数组
        props.put("mqtt.topic",mqttTopic);
        props.put("mqtt.broker","tcp://emqx:1883");
        props.put("mqtt.username","admin");
        props.put("mqtt.password","public");
        props.put("mqtt.clientid","client-i783e3d-1234");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        return props;
    }

}
