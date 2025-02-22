package com.dereckchen.remagen.kafka.interceptor;

import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.kakfa.restful.client.KafkaConnectManager;
import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.ConnectorInfoV2;
import com.dereckchen.remagen.utils.ConnectorUtils;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.MetricsUtils;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.dereckchen.remagen.utils.MetricsUtils.getLocalIp;

@Slf4j
public class KafkaBridgeConsumer extends KafkaConsumer<String, String> {


    private KafkaConnectManager connectManager;
    private Set<String> connectorNames = ConcurrentHashMap.newKeySet();
    private Histogram arriveAtKafkaLatency = MetricsUtils.getHistogram("arrive_at_kafka_latency",  "host");


    public KafkaBridgeConsumer(Map<String, Object> configs) {
        super(configs);
    }

    public KafkaBridgeConsumer(Map<String, Object> configs, String host, String port, boolean needHttps) {
        super(configs);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public KafkaBridgeConsumer(Properties properties) {
        super(properties);
    }

    public KafkaBridgeConsumer(Properties properties, String host, String port, boolean needHttps) {
        super(properties);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public KafkaBridgeConsumer(Properties properties, Deserializer<String> keyDeserializer, Deserializer<String> valueDeserializer) {
        super(properties, keyDeserializer, valueDeserializer);
    }

    public KafkaBridgeConsumer(Properties properties, Deserializer<String> keyDeserializer, Deserializer<String> valueDeserializer, String host, String port, boolean needHttps) {
        super(properties, keyDeserializer, valueDeserializer);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public KafkaBridgeConsumer(Map<String, Object> configs, Deserializer<String> keyDeserializer, Deserializer<String> valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }

    public KafkaBridgeConsumer(Map<String, Object> configs, Deserializer<String> keyDeserializer, Deserializer<String> valueDeserializer, String host, String port, boolean needHttps) {
        super(configs, keyDeserializer, valueDeserializer);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public void subscribe(Collection<String> topics, BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(topics);
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback, BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(topics, callback);
    }


    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback, BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(pattern, callback);
    }


    public void subscribe(Pattern pattern, BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(pattern);
    }

    @Override
    public ConsumerRecords<String, String> poll(Duration timeout) {
        ConsumerRecords<String, String> bridgeRecords = super.poll(timeout);
        LocalDateTime now = LocalDateTime.now();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>(16);
        for (TopicPartition topicPartition : bridgeRecords.partitions()) {
            List<ConsumerRecord<String, String>> records = bridgeRecords.records(topicPartition);
            List<ConsumerRecord<String, String>> originRecords = new ArrayList<>(records.size());
            for (ConsumerRecord<String, String> record : records) {
                BridgeMessage bridgeMessage = JsonUtils.fromJson(record.value(), BridgeMessage.class);
                if (bridgeMessage.getPubFromSource() != null) {
                    MetricsUtils.observeRequestLatency(arriveAtKafkaLatency, Duration.between(bridgeMessage.getArriveAtSource(), now).toMillis(), getLocalIp());
                }
                ConsumerRecord<String, String> newRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                        record.timestamp(), record.timestampType(), record.serializedKeySize(),
                        bridgeMessage.getContent().length(), record.key(), bridgeMessage.getContent(),
                        record.headers(), record.leaderEpoch());
                originRecords.add(newRecord);
            }
            recordsMap.put(topicPartition, originRecords);
        }
        return new ConsumerRecords<>(recordsMap);
    }

    @Override
    public void unsubscribe() {
        super.unsubscribe();
        for (String connectorName : connectorNames) {
            try {
                connectManager.deleteConnector(connectorName);
            } catch (Exception e) {
                log.error("delete connector {} failed", connectorName, e);
            }
        }
        connectorNames.clear();
    }


    public void checkConnector(BridgeOption option) {
        String connectorName = ConnectorUtils.getConnectorName(option.getMqttTopic(), option.getKafkaTopic());
        List<String> allConnectors = connectManager.getAllConnectors();
        if (!allConnectors.contains(connectorName)) {
            log.warn("Connector {} not exists", connectorName);
            ConnectorInfoV2 connector = connectManager.createConnector(connectorName, option.getProps());
            if (connector.getErrorCode() != null) {
                log.error("create connector {} failed, errorMessage:{}", connectorName, connector.getMessage());
                throw new RetryableException("create connector failed");
            }
            log.info("create connector {} success", connectorName);
        }
        connectorNames.add(connectorName);
    }
}
