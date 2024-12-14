package com.dereckchen.remagen.kafka.connector.source;

import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.models.BridgeConfig;
import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.MQTTConfig;
import com.dereckchen.remagen.models.Pair;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.KafkaUtils;
import com.dereckchen.remagen.utils.MQTTUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.eclipse.paho.client.mqttv3.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.dereckchen.remagen.utils.ConnectorUtils.parseConfig;
import static com.dereckchen.remagen.utils.KafkaUtils.getPartition;
import static com.dereckchen.remagen.utils.MQTTUtil.defaultOptions;
import static com.dereckchen.remagen.utils.MQTTUtil.tryReconnect;

@Slf4j
public class MqttSourceTask extends SourceTask {


    private BridgeConfig config;

    private MqttClient client;

    private ArrayDeque<SourceRecord> records;
    private ReentrantLock lock;
    private Map<SourceRecord, Pair<Integer, Integer>> mqttIdMap;
    private AtomicBoolean running;
    private String latestTimeStamp;

    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting MqttSourceTask with properties: {}", map); // 打印所有属性
        config = parseConfig(map);

        // 内部对象实例化
        running = new AtomicBoolean(true);
        records = new ArrayDeque<>();
        lock = new ReentrantLock(true);
        mqttIdMap = new ConcurrentHashMap<>(); // thread safe, remove key freely
        latestTimeStamp = "";

        // 初始化mqtt客户端
        initializeMqttClient();

        // 更新offset
        getOffset();

        // 设置回调
        setCallback();

        // 开始监听
        subscribe();
    }

    public void subscribe() {
        try {
            client.subscribe(config.getMqttConfig().getTopic(), 0);
        } catch (MqttException e) {
            log.error("Error subscribing to topic", e);
            throw new RuntimeException(e);
        }
    }


    public void getOffset() {
        OffsetStorageReader offsetStorageReader = context.offsetStorageReader();
        // 获取偏移量
        Map<String, Object> offset = offsetStorageReader.offset(
                getPartition(config.getKafkaServerConfig().getKafkaTopic(),
                        config.getMqttConfig().getTopic()));
        log.info("offset from kafka: {}", offset);
        if (offset != null) {
            latestTimeStamp = (String) offset.getOrDefault(
                    "timestamp", ""); // 恢复偏移
        }
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
            client = MQTTUtil.getMqttClient(config.getMqttConfig());
            MqttConnectOptions mqttConnectOptions = MQTTUtil.defaultOptions(config.getMqttConfig());
            client.connect(mqttConnectOptions);
        } catch (Exception e) {
            log.error("Error initializing MQTT client", e);
            throw new RuntimeException(e);
        }
    }

    public void setCallback() {
        client.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) {
                log.error("connectionLost: {}", cause.getMessage(), cause);
                MQTTConfig mqttConfig = config.getMqttConfig();
                synchronized (client) {
                    tryReconnect(running::get, client, defaultOptions(mqttConfig), mqttConfig);
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                log.info("Received topic[{}] message: {} ", topic, message);

                BridgeMessage bridgeMessage = JsonUtils.fromJson(message.getPayload(), BridgeMessage.class);
                if (bridgeMessage.getTimestamp().compareTo(latestTimeStamp) <= 0) {
                    client.messageArrivedComplete(message.getId(), message.getQos());
                    log.warn("Ignore old message. ts:{}  lastTimeStamp{}", bridgeMessage.getTimestamp(), latestTimeStamp);
                    return; // ignore old messages
                }

                lock.lock();
                SourceRecord sourceRecord = new SourceRecord(
                        KafkaUtils.getPartition(config.getKafkaServerConfig().getKafkaTopic(), topic),
                        Collections.singletonMap(ConnectorConst.OFFSET_TIMESTAMP_KEY, bridgeMessage.getTimestamp()),
                        config.getKafkaServerConfig().getKafkaTopic(), null, bridgeMessage.getContent());
                records.add(sourceRecord);
                mqttIdMap.put(sourceRecord, new Pair<>(message.getId(), message.getQos()));
                lock.unlock();
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
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
        Pair<Integer, Integer> idAndQos = mqttIdMap.get(record);
        try {
            client.messageArrivedComplete(idAndQos.getKey(), idAndQos.getValue()); // ack the mqtt-msg
            log.info("Ack mqtt message with id: {}", idAndQos.getKey());
            log.info("Success send record: {}", record.value());
        } catch (MqttException e) {
            log.error("Ack mqtt message failed for record:{}, mqtt-msgId:{}", record, idAndQos.getKey(), e);
            throw new RuntimeException(e);
        }
    }

}
