package com.dereckchen.remagen.demo;

import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttBridgeClient;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MqttSubscribe {
    public static void main(String[] args) throws Exception {
        String serverURI = "tcp://localhost:1883";
        String clientId = "cccmqtt-source-demo" + Math.random();
        String mqtt_topic = "test_mqtt";
        String kafka_topic = "sync_topic";

        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().host("127.0.0.1").port("38083").needHttps(false).build();
        MqttBridgeClient mqttBridgeClient = new MqttBridgeClient(serverURI, clientId, kafkaServerConfig);
        mqttBridgeClient.connect();


        Map<String, String> props = getProps(mqtt_topic, kafka_topic, clientId);

        BridgeOption bridgeOption = new BridgeOption(mqtt_topic, kafka_topic, props);

        mqttBridgeClient.subscribe(new BridgeOption[]{bridgeOption}, new int[]{0}, new IMqttMessageListener[]{(topic, message) -> {
            log.info("topic {}, receive msg: {}", topic, message.toString());
        }});
    }

    private static Map<String, String> getProps(String mqtt_topic, String kafka_topic, String clientId) {
        Map<String, String> props = new HashMap<>();
        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.sink.MqttSinkConnector");
        props.put("tasks.max", "1");
        props.put("topics", kafka_topic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.topic", mqtt_topic);
        props.put("mqtt.password", "public");
        props.put("mqtt.username", "admin");
        props.put("mqtt.clientid", clientId + "1234");
        return props;
    }
}
