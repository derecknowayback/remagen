package com.dereckchen.remagen.benchmark;

import com.dereckchen.remagen.kafka.interceptor.KafkaBridgeConsumer;
import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.IBridgeMessageContent;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.mqtt.client.MqttBridgeClient;
import com.dereckchen.remagen.utils.MetricsUtils;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SourceTaskLatency {

    // 整体链路
    // mqtt发送消息 -> source connector -> kafka topic
    // 框架发送消息
    // 程序回收消息

    public static void main(String[] args) throws Exception {
        // 主题名称
        String topic = "syn_topic213";
        String mqttTopic = "test_mqtt345";

        // 创建消费者实例
        KafkaBridgeConsumer consumer = getConsumer();
        BridgeOption kafkaBridgeOption = getKafkaBridgeOption(topic, mqttTopic);
        consumer.subscribe(Collections.singletonList(topic), kafkaBridgeOption);

        // 创建生产者实例
        BridgeOption mqttBridgeOption = getMqttBridgeOption(topic, mqttTopic);
        MqttBridgeClient mqttBridgeClient = getMqttClient();

        MetricsUtils.FlushGatewayThread gatewayThread = new MetricsUtils.FlushGatewayThread(new PushGateway("localhost:9091"));
        Thread pushThread = new Thread(gatewayThread);
        pushThread.start();

        Runnable send = sendOnce(mqttBridgeClient, mqttTopic, mqttBridgeOption);
        Runnable receive = pollReceive(consumer);
        Thread consumerThread = new Thread(receive);
        consumerThread.start();

        while (true) {
            log.info("请输入 QPS（输入 0 退出）: ");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input = reader.readLine();
            int qps = Integer.parseInt(input);
            if (qps == 0) {
                log.info("退出");
                break;
            }

            LocalDateTime future = LocalDateTime.now().plusMinutes(10);
            while (future.isAfter(LocalDateTime.now())) {
                DynamicQPSTest.testQPS(send, qps);
            }
        }

        pushThread.interrupt();
        consumerThread.interrupt();
    }

    private static Runnable sendOnce(MqttBridgeClient mqttBridgeClient, String mqttTopic, BridgeOption mqttBridgeOption) {
        return () -> {
            BridgeMessage msg = new BridgeMessage(new IBridgeMessageContent() {
                @Override
                public String serializeToJsonStr() {
                    return json;
                }

                @Override
                public String getMessageId() {
                    return "id_" + Math.random();
                }
            }, 0, true);
            try {
                mqttBridgeClient.publish(mqttTopic, msg, mqttBridgeOption);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        };
    }

    final static String json = "{\n" +
            "  \"product\": {\n" +
            "    \"id\": 1001,\n" +
            "    \"name\": \"Wireless Bluetooth Headphones\",\n" +
            "    \"price\": 59.99,\n" +
            "    \"isAvailable\": true,\n" +
            "    \"description\": \"High-quality wireless headphones with noise cancellation and comfortable fit.\",\n" +
            "    \"weight\": 0.25,\n" +
            "    \"dimensions\": {\n" +
            "      \"length\": 15,\n" +
            "      \"width\": 12,\n" +
            "      \"height\": 5\n" +
            "    },\n" +
            "    \"color\": [\"black\", \"white\", \"blue\"],\n" +
            "    \"brand\": \"TechGadgets\",\n" +
            "    \"category\": \"Electronics\",\n" +
            "    \"manufacturedDate\": \"2023-08-15\",\n" +
            "    \"expiryDate\": \"2025-08-15\",\n" +
            "    \"isOrganic\": false,\n" +
            "    \"isImported\": true,\n" +
            "    \"isLocallySourced\": false,\n" +
            "    \"isOnSale\": true,\n" +
            "    \"salePercentage\": 15,\n" +
            "    \"isSoldOut\": false,\n" +
            "    \"isReturnable\": true,\n" +
            "    \"isRecyclable\": true,\n" +
            "    \"isCustomizable\": false,\n" +
            "    \"isSubscriptionAvailable\": false,\n" +
            "    \"isBackordered\": false,\n" +
            "    \"isDiscontinued\": false,\n" +
            "    \"isInStock\": true,\n" +
            "    \"isPreorder\": false,\n" +
            "    \"isClearance\": false,\n" +
            "    \"isSeasonal\": false,\n" +
            "    \"isGiftWrapped\": false,\n" +
            "    \"isRefurbished\": false,\n" +
            "    \"isNew\": true,\n" +
            "    \"isUsed\": false,\n" +
            "    \"isCertifiedRefurbished\": false,\n" +
            "    \"isOpenBox\": false,\n" +
            "    \"isBulkAvailable\": true,\n" +
            "    \"isPersonalizable\": false,\n" +
            "    \"isMonogrammable\": false,\n" +
            "    \"isEngraved\": false,\n" +
            "    \"isCustomMade\": false,\n" +
            "    \"isHandmade\": false,\n" +
            "    \"isVegan\": false,\n" +
            "    \"isCrueltyFree\": false,\n" +
            "    \"isHypoallergenic\": false,\n" +
            "    \"isFragranceFree\": false,\n" +
            "    \"isParabenFree\": false,\n" +
            "    \"isSulfateFree\": false,\n" +
            "    \"isAlcoholFree\": false,\n" +
            "    \"isDyeFree\": false,\n" +
            "    \"isPabaFree\": false,\n" +
            "    \"isNonComedogenic\": false,\n" +
            "    \"isOilFree\": false,\n" +
            "    \"isWaterResistant\": false,\n" +
            "    \"isWaterproof\": false,\n" +
            "    \"isSweatProof\": false,\n" +
            "    \"isDustProof\": false,\n" +
            "    \"isShockProof\": false,\n" +
            "    \"isScratchResistant\": false,\n" +
            "    \"isShatterproof\": false,\n" +
            "    \"isFreezable\": false,\n" +
            "    \"isMicrowaveSafe\": false,\n" +
            "    \"isDishwasherSafe\": false,\n" +
            "    \"isOvenSafe\": false,\n" +
            "    \"isStovetopSafe\": false,\n" +
            "    \"isFreezerToOven\": false,\n" +
            "    \"isFreezerToMicrowave\": false,\n" +
            "    \"isFreezerToDishwasher\": false,\n" +
            "    \"isFreezerToStovetop\": false,\n" +
            "    \"isOvenToTable\": false,\n" +
            "    \"isMicrowaveToTable\": false,\n" +
            "    \"isDishwasherToTable\": false,\n" +
            "    \"isStovetopToTable\": false,\n" +
            "    \"isFreezerToTable\": false,\n" +
            "    \"isOvenToFreezer\": false,\n" +
            "    \"isMicrowaveToFreezer\": false,\n" +
            "    \"isDishwasherToFreezer\": false,\n" +
            "    \"isStovetopToFreezer\": false,\n" +
            "    \"isTableToFreezer\": false,\n" +
            "    \"isTableToOven\": false,\n" +
            "    \"isTableToMicrowave\": false,\n" +
            "    \"isTableToDishwasher\": false,\n" +
            "    \"isTableToStovetop\": false\n" +
            "  }\n" +
            "}";

    final static AtomicInteger counter = new AtomicInteger(0);
    private static Runnable pollReceive(KafkaBridgeConsumer consumer) {
        return () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty() && counter.incrementAndGet() % 100 == 0) {
                        log.info("Received message size: {}", records.count());
                        counter.set(0);
                    }
                }
            } finally {
                // 关闭消费者
                try {
                    consumer.close();
                } catch (Exception e) {
                    log.error("Failed to close consumer", e);
                }

            }
        };
    }


    static KafkaBridgeConsumer getConsumer() {
        // Kafka broker地址
        String bootstrapServers = "localhost:39092,localhost:39093,localhost:39094";
        // 消费者组ID
        String groupId = "my-group";

        // 配置消费者
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); // 从最早的消息开始消费
        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");

        // 创建消费者实例
        String host = "localhost", port = "38083";
        return new KafkaBridgeConsumer(props, host, port, false);
    }

    static BridgeOption getKafkaBridgeOption(String topic, String mqttTopic) {
        BridgeOption bridgeOption = new BridgeOption();
        bridgeOption.setKafkaTopic(topic);
        bridgeOption.setMqttTopic(mqttTopic);
        Map<String, String> props = new HashMap<>();
        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.source.MqttSourceConnector");
        props.put("tasks.max", "1");
        props.put("topic", topic);
        props.put("mqtt.topic", mqttTopic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.username", "admin");
        props.put("mqtt.password", "public");
        props.put("mqtt.clientid", "client-i783e3d-12345");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        bridgeOption.setProps(props);
        return bridgeOption;
    }


    static MqttBridgeClient getMqttClient() throws MqttException {
        String serverURI = "tcp://localhost:1883";
        String clientId = "cccmqtt-1" + Math.random();
        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().host("127.0.0.1").port("38083").needHttps(false).build();

        MqttBridgeClient mqttBridgeClient = new MqttBridgeClient(serverURI, clientId, kafkaServerConfig);
        mqttBridgeClient.connect();
        return mqttBridgeClient;
    }

    static BridgeOption getMqttBridgeOption(String topic, String mqttTopic) {
        Map<String, String> props = new HashMap<>();
        props.put("kafkaConnectManager.host", "127.0.0.1");
        props.put("kafkaConnectManager.port", "38083");
        props.put("kafkaConnectManager.needHttps", "false");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("connector.class", "com.dereckchen.remagen.kafka.connector.source.MqttSourceConnector");
        props.put("tasks.max", "1");
        props.put("topic", topic);
        props.put("mqtt.broker", "tcp://emqx:1883");
        props.put("mqtt.topic", mqttTopic);
        props.put("mqtt.password", "public");
        props.put("mqtt.username", "admin");
        props.put("mqtt.clientid", Math.random() * Math.random() + "1234");
        return new BridgeOption(mqttTopic, topic, props);
    }


}
