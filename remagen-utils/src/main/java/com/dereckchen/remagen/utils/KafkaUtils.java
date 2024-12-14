package com.dereckchen.remagen.utils;

import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.models.KafkaServerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.dereckchen.remagen.consts.ConnectorConst.PARTITION_KAFKA_TOPIC_KEY;
import static com.dereckchen.remagen.consts.ConnectorConst.PARTITION_MQTT_TOPIC_KEY;

public class KafkaUtils {
    public static KafkaServerConfig parseConfig(Map<String, String> prop) {
        return KafkaServerConfig.builder()
                .host(prop.get(ConnectorConst.PROPS_KAFKA_HOST))
                .port(prop.get(ConnectorConst.PROPS_KAFKA_PORT))
                .needHttps(Boolean.parseBoolean(prop.getOrDefault(ConnectorConst.PROPS_KAFKA_NEED_HTTPS, "false")))
                .kafkaTopic(prop.getOrDefault(ConnectorConst.PROPS_KAFKA_TOPIC, ""))
                .build();
    }

    public static Map<String, String> getPartition(String kafkaTopic, String mqttTopic) {
        return Collections.unmodifiableMap(
                new HashMap<String, String>() {{
                    put(PARTITION_KAFKA_TOPIC_KEY, kafkaTopic);
                    put(PARTITION_MQTT_TOPIC_KEY, mqttTopic);
                }}
        );
    }
}
