package com.dereckchen.remagen.kafka.connector.source;

import com.dereckchen.remagen.kafka.connector.models.MQTTConfig;
import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class MqttSourceTask extends SourceTask {


    private static final int MAX_RETRY_COUNT = 10;

    private static final String KAFKA_PROP_KEY = "topic";

    private static final String PARTITION_KAFKA_TOPIC_KEY = "kafkaTopic";
    private static final String PARTITION_MQTT_TOPIC_KEY = "mqttTopic";

    private Map<String, String> props;
    private String kafkaTopic;

    private AtomicBoolean running;
    private AtomicInteger retryCount;

    private MqttClient client;
    private MQTTConfig mqttConfig;
    private MqttConnectOptions options;

    private ArrayDeque<SourceRecord> records;
    private ReentrantLock lock;
    private Map<SourceRecord, Pair<Integer, Integer>> mqttIdMap;

    private String latestTimeStamp = "";


    private static class Pair<K,V> {
        private K key;
        private V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> map) {
        props = map;
        running = new AtomicBoolean(true);
        retryCount = new AtomicInteger(0);
        records = new ArrayDeque<>();
        lock = new ReentrantLock(true);
        mqttIdMap = new HashMap<>();

        // 初始化mqtt客户端
        initializeMqttClient();

        // 获取偏移量
        Map<String, Object> offset = context.offsetStorageReader().offset(getPartition());
        log.info("offset: {}", offset);
        if (offset != null) {
            latestTimeStamp = (String) offset.getOrDefault("timestamp",""); // 恢复偏移
        } else {
            latestTimeStamp = "";  // 如果没有偏移，选择从头开始
        }


        setCallback();
        log.info("Start listening to topics {}", mqttConfig.getTopic());
        try {
            client.subscribe(mqttConfig.getTopic(), 0);
        } catch (MqttException e) {
            log.error("Error subscribing to topic", e);
            throw new RuntimeException(e);
        }


        // 打印所有属性
        log.info("Starting MqttSourceTask with properties: {}" , map);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!running.get()) {
            return null;
        }
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        lock.lock();
        log.info("Returning {} records", records.size());
        if (records.isEmpty()) {
            lock.unlock();
            return Collections.emptyList();
        }
        List<SourceRecord> sourceRecords = new ArrayList<>(records);
        records.clear();
        lock.unlock();

        return sourceRecords;
    }


    public void initializeMqttClient() {
        try {
            mqttConfig = parseConfig(props);
            kafkaTopic = props.get(KAFKA_PROP_KEY);
            client = new MqttClient(mqttConfig.getBroker(), mqttConfig.getClientid(), new MemoryPersistence());
            client.setManualAcks(true);

            // 连接参数
            options = new MqttConnectOptions();
            options.setUserName(mqttConfig.getUsername());
            options.setPassword(mqttConfig.getPassword().toCharArray());
            options.setConnectionTimeout(0);
            options.setKeepAliveInterval(0);
            options.setAutomaticReconnect(true);

            client.connect(options);
        } catch (Exception e) {
            log.error("Error initializing MQTT client", e);
            throw new RuntimeException(e);
        }
    }


    public void setCallback() {
        client.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) {
                log.error("connectionLost: {}", cause.getMessage(), cause);
                tryReconnect();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                log.info("Received message: {}", message);
                BridgeMessage bridgeMessage = JsonUtils.fromJson(message.getPayload(), BridgeMessage.class);
                if (bridgeMessage.getTimestamp().compareTo(latestTimeStamp) <= 0) {
                    client.messageArrivedComplete(message.getId(), message.getQos());
                    log.warn("Ignore old message. ts:{}  lastTimeStamp{}", bridgeMessage.getTimestamp(), latestTimeStamp);
                    return; // ignore old messages
                }

                lock.lock();
                SourceRecord sourceRecord = new SourceRecord(
                        getPartition(),
                        Collections.singletonMap("timestamp", bridgeMessage.getTimestamp()),
                        kafkaTopic, null, bridgeMessage.getContent());
                records.add(sourceRecord);
                mqttIdMap.put(sourceRecord, new Pair<>(message.getId(), message.getQos()));
                lock.unlock();
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
    }

    public void tryReconnect () {
        // 尝试重连
        while (running.get()) {
            int countTmp = retryCount.incrementAndGet();
            try {
                log.info("Trying to connect the mqttServer ....");
                log.info("Options: {}", options);
                log.info("Config: {}",mqttConfig);
                client.connect(options);
                client.subscribe(mqttConfig.getTopic(), 0);
                log.info("Re-connect the mqttServer success ....");
                break;
            } catch (Exception e) {
                log.error("Retry failed for {} times", countTmp, e);
                if (countTmp == MAX_RETRY_COUNT) {
                    log.error("Retry failed for {} times, giving up...", countTmp);
                    running.set(false);
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    log.error("Wait time exception...", ex);
                }
            }
        }
        retryCount.set(0); // reset counter
    }


    public Map<String, String> getPartition() {
        return Collections.unmodifiableMap(
                new HashMap<String, String>() {{
                    put(PARTITION_KAFKA_TOPIC_KEY, kafkaTopic);
                    put(PARTITION_MQTT_TOPIC_KEY, mqttConfig.getTopic());
                }}
        );
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
    public void stop() {
        try {
            client.close();
            running.set(false);
        } catch (MqttException e) {
            log.error("Close mqttClient failed...");
            throw new RuntimeException(e);
        }
        log.info("Stopped MQTT Source Task");
    }


    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        super.commitRecord(record, metadata); // actually do nothing, just nop
        try {
            Pair<Integer, Integer> idAndQos = mqttIdMap.get(record);
            client.messageArrivedComplete(idAndQos.getKey(), idAndQos.getValue()); // ack the mqtt-msg
            log.info("Ack mqtt message with id: {}", idAndQos.getKey());
            log.info("Success send record: {}",record.value());

        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public void monitor () {
        // 创建一个子线程不断发送消息
        new Thread(() -> {
            while (true) {
                log.info("Monitoring... :{}",client.isConnected());
                if (!client.isConnected()) {
                    tryReconnect();
                }
                try {
                    client.publish("monitor", "hello world".getBytes(), 0, false);
                } catch (MqttException e) {
                    log.error("Monitoring failed...", e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    log.error("Interrupted...");
                }
            }
        }).start();
    }

}
