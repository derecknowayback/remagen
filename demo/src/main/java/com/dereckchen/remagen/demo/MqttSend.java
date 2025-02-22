package com.dereckchen.remagen.demo;

import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.IBridgeMessageContent;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttBridgeClient;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MqttSend {

    public static void main(String[] args) throws Exception {
        String serverURI = "tcp://localhost:1883";
        String clientId = "cccmqtt-1" + Math.random();
        String mqtt_topic = "mqtt_test_cjp1";
        String kafka_topic = "sync_topic";

        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().host("127.0.0.1").port("38083").needHttps(false).build();

        String json = "{\n" +
                "\"uid\":1,\n" +
                "\"cost\":100\n" +
                "}";

        MqttBridgeClient mqttBridgeClient = new MqttBridgeClient(serverURI, clientId, kafkaServerConfig);
        mqttBridgeClient.connect();

        Map<String, String> props = getProps(mqtt_topic, kafka_topic, clientId);

        BridgeOption bridgeOption = new BridgeOption(mqtt_topic, kafka_topic, props);

        while (true){
            BridgeMessage msg = new BridgeMessage(new IBridgeMessageContent() {
                @Override
                public String serializeToJsonStr() {
                    return json;
                }

                @Override
                public String getMessageId() {
                    return "1" + Math.random() + Math.random();
                }
            }, 0, true);
            mqttBridgeClient.publish(mqtt_topic, msg, bridgeOption);
            log.info("send msg: {}", msg);
            Thread.sleep(1000);
        }
    }

    private static Map<String, String> getProps(String mqtt_topic, String kafka_topic, String clientId) {
        Map<String, String> props = new HashMap<>();
        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.source.MqttSourceConnector");
        props.put("tasks.max", "1");
        props.put("topic", kafka_topic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.topic", mqtt_topic);
        props.put("mqtt.password", "public");
        props.put("mqtt.username", "admin");
        props.put("mqtt.clientid", clientId + "1234");
        return props;
    }
}
