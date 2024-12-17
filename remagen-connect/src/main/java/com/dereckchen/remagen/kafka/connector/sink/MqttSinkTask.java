package com.dereckchen.remagen.kafka.connector.sink;


import com.dereckchen.remagen.models.MQTTConfig;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.MQTTUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MqttSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MqttSinkTask.class);

    // 连接 有状态
    private MqttClient mqttClient;
    private MQTTConfig config;


    @Override
    public String version() {
        return "unknown";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Task start... Using props: {}", props);

        config = parseConfig(props);
        // 2. 设置状态量
        log.info("Successfully started kafka-source task...");
    }

    public MQTTConfig parseConfig(Map<String, String> props) {
        return MQTTConfig.builder()
                .password(props.getOrDefault("mqtt.password", ""))
                .clientid(props.getOrDefault("mqtt.clientid", ""))
                .username(props.getOrDefault("mqtt.username", ""))
                .broker(props.getOrDefault("mqtt.broker", ""))
                .topic(props.getOrDefault("mqtt.topic", "")).build();
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Start polling record from Kafka...");

        if (records == null || records.isEmpty()) {
            log.info("Polled empty records from Kafka.");
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.info(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );

        sendMQTT(records);
    }

    private void sendMQTT(Collection<SinkRecord> records) {
        records.forEach(record -> {
            try {
                Object obj = record.value();
                MqttMessage mqttMessage = new MqttMessage(JsonUtils.toJsonBytes(obj));
                if (mqttClient == null) {
                    initMqttClient();
                    mqttClient.publish(config.getTopic(), mqttMessage);
                } else {
                    mqttClient.publish(config.getTopic(), mqttMessage);
                }
            } catch (MqttException e) {
                log.error("Send mqtt message error", e);
                throw new RuntimeException(e);
            }
        });
    }

    public void initMqttClient () throws MqttException {
        mqttClient = MQTTUtil.getMqttClient(config);
        MqttConnectOptions mqttConnectOptions = MQTTUtil.defaultOptions(config);
        mqttClient.connect(mqttConnectOptions);
    }

    @Override
    public void stop() {
        log.info("Closing resources...");
        // 关闭MQTT客户端
        try {
            mqttClient.close();
            log.info("Successfully closed mqtt client...");
        } catch (MqttException e) {
            log.error("Close mqttClient error", e);
            throw new RuntimeException(e);
        }
    }
}
