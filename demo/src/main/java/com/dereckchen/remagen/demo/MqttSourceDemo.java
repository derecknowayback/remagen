package com.dereckchen.remagen.demo;

import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.IBridgeMessageContent;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttClientV2;

import java.util.HashMap;
import java.util.Map;

public class MqttSourceDemo {

    public static void main(String[] args) throws Exception {
        String serverURI = "tcp://localhost:1883";
        String clientId = "cccmqtt-source-demo" + Math.random();
        String mqtt_topic = "mqtt_test_cjp1";
        String kafka_topic = "mys";

        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().host("127.0.0.1").port("8848").needHttps(false).build();

        String json = "{\n" +
                "\"uid\":1,\n" +
                "\"cost\":100\n" +
                "}";

        MqttClientV2 mqttClientV2 = new MqttClientV2(serverURI, clientId, kafkaServerConfig);
        mqttClientV2.connect();


        Map<String, String> props = getProps(mqtt_topic, kafka_topic, clientId);

        BridgeOption bridgeOption = new BridgeOption(mqtt_topic, kafka_topic, props);

        for (int i = 0; i < 20; i++) {
            BridgeMessage msg = new BridgeMessage(new IBridgeMessageContent() {
                @Override
                public String serializeToJsonStr() {
                    return json;
                }

                @Override
                public String getMessageId() {
                    return "1";
                }
            }, 0, true);
            mqttClientV2.publish(mqtt_topic, msg, bridgeOption);
        }
    }

    private static Map<String, String> getProps(String mqtt_topic, String kafka_topic, String clientId) {
        Map<String, String> props = new HashMap<>();
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