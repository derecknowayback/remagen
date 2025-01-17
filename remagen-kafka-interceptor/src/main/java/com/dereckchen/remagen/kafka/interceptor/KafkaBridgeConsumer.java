package com.dereckchen.remagen.kafka.interceptor;

import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.kakfa.restful.client.KafkaConnectManager;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.ConnectorInfoV2;
import com.dereckchen.remagen.utils.ConnectorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

@Slf4j
public class KafkaBridgeConsumer<K,V> extends KafkaConsumer<K,V> {


    private KafkaConnectManager connectManager;
    private Set<String> connectorNames = ConcurrentHashMap.newKeySet();


    public KafkaBridgeConsumer(Map<String, Object> configs) {
        super(configs);
    }
    public KafkaBridgeConsumer (Map<String, Object> configs,String host, String port, boolean needHttps) {
        super(configs);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }



    public KafkaBridgeConsumer(Properties properties) {
        super(properties);
    }
    public KafkaBridgeConsumer(Properties properties,String host, String port, boolean needHttps) {
        super(properties);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public KafkaBridgeConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(properties, keyDeserializer, valueDeserializer);
    }
    public KafkaBridgeConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,String host, String port, boolean needHttps) {
        super(properties, keyDeserializer, valueDeserializer);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }



    public KafkaBridgeConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }
    public KafkaBridgeConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,String host, String port, boolean needHttps) {
        super(configs, keyDeserializer, valueDeserializer);
        connectManager = new KafkaConnectManager(host, port, needHttps);
    }


    public void subscribe(Collection<String> topics, BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(topics);
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback,BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(topics, callback);
    }


    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback,BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(pattern, callback);
    }


    public void subscribe(Pattern pattern,BridgeOption bridgeOption) {
        checkConnector(bridgeOption);
        super.subscribe(pattern);
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


    public void checkConnector (BridgeOption option) {
        String connectorName = ConnectorUtils.getConnectorName(option.getMqttTopic(), option.getKafkaTopic());
        List<String> allConnectors = connectManager.getAllConnectors();
        if (!allConnectors.contains(connectorName)) {
            log.warn("Connector {} not exists", connectorName);
            ConnectorInfoV2 connector = connectManager.createConnector(connectorName, option.getProps());
            if (connector.getErrorCode() == null) {
                log.error("create connector {} failed, errorMessage:{}", connectorName,  connector.getMessage());
                throw new RetryableException("create connector failed");
            }
            log.info("create connector {} success", connectorName);
        }
        connectorNames.add(connectorName);
    }



}
