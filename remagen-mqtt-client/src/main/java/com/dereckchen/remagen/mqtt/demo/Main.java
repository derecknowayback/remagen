package com.dereckchen.remagen.mqtt.demo;

import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttClientV2;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Main {
    public static void main(String[] args) throws MqttException {
        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder()
                .host("127.0.0.1").port("8848").needHttps(false).build();

        String broker = "tcp://localhost:1883";
        String topic = "mqtt_test_cjp";
        String username = "admin";
        String password = "public";
        String clientid = "ccclient1";

        MqttClientV2 mqttClientV2 = new MqttClientV2(broker, clientid, kafkaServerConfig);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(60);
        options.setAutomaticReconnect(true);
        mqttClientV2.connect(options);

        byte[] json = JsonUtils.toJsonBytes(Collections.unmodifiableMap(
                new HashMap<String, String>() {{
                    put("Name", "Tim Duncan");
                    put("Team", "Spurs");
                }}
        ));


        Map<String, String> props = Collections.unmodifiableMap(
                new HashMap<String, String>() {{
                    put("connector.class", "com.dereckchen.kafka.connector.sink.MqttSinkConnector");
                    put("tasks.max", "1");
                    put("topics", topic);
                }}
        );
        BridgeOption bridgeOption = new BridgeOption(topic, "mysink", props);


        log.info("Success send");

        mqttClientV2.unsubscribe(bridgeOption);
        log.info("Unsubscribe ok");
    }
}
