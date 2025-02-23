package com.dereckchen.remagen.benchmark;

import com.dereckchen.remagen.kafka.interceptor.KafkaBridgeProducer;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttBridgeClient;
import com.dereckchen.remagen.utils.MetricsUtils;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class SinkTaskLatency {
    // 总体流程
    // kafka -> sink connector -> mqtt
    // 框架发送消息
    // 程序回收消息

    public static void main(String[] args) throws Exception {
        String serverURI = "tcp://localhost:1883";
        String clientId = "cccmqtt-source-demo" + Math.random();
        String mqtt_topic = "test_mqtt1";
        String kafka_topic = "sync_topic2";
        String bootstrapServers = "localhost:39092,localhost:39093,localhost:39094";

        // kafka 生产者
        KafkaBridgeProducer producer = getProducer(bootstrapServers);
        Runnable send = sendOnce(kafka_topic, mqtt_topic, getProps(kafka_topic, mqtt_topic), producer);

        // mqtt 消费者
        BridgeOption mqttBridgeOption = getMqttBridgeOption(mqtt_topic, kafka_topic, clientId);
        MqttBridgeClient mqttClient = getMqttClient(serverURI, clientId);
        mqttClient.subscribe(new BridgeOption[]{mqttBridgeOption}, new int[]{0}, null);

        MetricsUtils.FlushGatewayThread gatewayThread = new MetricsUtils.FlushGatewayThread(new PushGateway("localhost:9091"));
        Thread pushThread = new Thread(gatewayThread);
        pushThread.start();

        while (true) {
            log.info("请输入 QPS（输入 0 退出）: ");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input = reader.readLine();
            int qps = Integer.parseInt(input);
            if (qps == 0) {
                log.info("退出");
                break;
            }
            DynamicQPSTest.testQPS(send, qps);
            if (!mqttClient.isConnected()) {
                log.info("mqtt 断开连接");
            }
        }
        pushThread.interrupt();
    }

    static BridgeOption getMqttBridgeOption(String mqtt_topic, String kafka_topic, String clientId) {
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
        return new BridgeOption(mqtt_topic, kafka_topic, props);
    }

    static MqttBridgeClient getMqttClient(String serverURI, String clientId) throws MqttException {
        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().host("127.0.0.1").port("38083").needHttps(false).build();
        MqttBridgeClient mqttBridgeClient = new MqttBridgeClient(serverURI, clientId, kafkaServerConfig);
        mqttBridgeClient.connect();
        return mqttBridgeClient;
    }

    static KafkaBridgeProducer getProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.dereckchen.remagen.kafka.interceptor.BridgeProducerInterceptor");

        // Enable idempotence to handle duplicates
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Increase metadata refresh interval
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
        // More acknowledgments for reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Increase retries and backoff
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");

        return new KafkaBridgeProducer<>(props);
    }

    static Runnable sendOnce(String topic, String mqttTopic, Map<String, String> props, KafkaBridgeProducer producer) {
        return () -> {
            String key = Math.random() + "";
            String value = "{\n" +
                    "\"uid\":1,\n" +
                    "\"cost\":100\n" +
                    "}";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key + Math.random(), value);
            BridgeOption option = new BridgeOption(mqttTopic, topic, props);
            producer.send(record, "1" + Math.random(), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending message: {}", record, exception);
                }
            }, option);
        };
    }

    static Map<String, String> getProps(String topic, String mqttTopic) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.sink.MqttSinkConnector");
        props.put("tasks.max", "1");
        props.put("topics", topic);
        props.put("mqtt.topic", mqttTopic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.username", "admin");
        props.put("mqtt.password", "public");
        props.put("mqtt.clientid", "client-i783e3d-1234" + Math.random());
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        return props;
    }


}
