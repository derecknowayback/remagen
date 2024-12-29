package com.dereckchen.remagen.kafka.connector.source;

import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.exceptions.RetryableException;
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

import static com.dereckchen.remagen.consts.ConnectorConst.DEFAULT_VERSION;
import static com.dereckchen.remagen.consts.ConnectorConst.OFFSET_TIMESTAMP_KEY;
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
    private String kafkaTopic;

    @Override
    public String version() {
        // get version from env
        String VERSION = System.getenv("REMAGEN.VERSION");
        if (VERSION == null) {
            VERSION = DEFAULT_VERSION;
        }
        log.info("MqttSourceTask version: {}", VERSION);
        return VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        // Print all properties for logging and debugging purposes
        log.info("Starting MqttSourceTask with properties: {}", map);

        // Parse the configuration from the input map
        config = parseConfig(map);

        // Internal object instantiation
        running = new AtomicBoolean(true);
        records = new ArrayDeque<>();
        lock = new ReentrantLock(true);
        // Use ConcurrentHashMap for thread safety, allowing keys to be freely removed
        mqttIdMap = new ConcurrentHashMap<>();
        latestTimeStamp = "";

        // Get kafka topic
        String kafkaTopics = config.getKafkaServerConfig().getKafkaTopics();
        kafkaTopic = getKafkaTopic(kafkaTopics);

        // Initialize the MQTT client
        initializeMqttClient();

        // Update the offset
        String mqttTopic = config.getMqttConfig().getTopic();
        latestTimeStamp = getOffset(kafkaTopic, mqttTopic);

        // Set the callback
        setCallback();

        // Start listening
        subscribe();
    }


    /**
     * Subscribes to the topic specified in the configuration.
     * This method is used to subscribe to the MQTT topic.
     * If an error occurs while subscribing, a RetryableException is thrown.
     */
    public void subscribe() {
        try {
            // Subscribe to the topic with QoS level 0
            client.subscribe(config.getMqttConfig().getTopic(), 0);
        } catch (MqttException e) {
            // Log the error message
            log.error("Error subscribing to topic", e);
            // Throw a RetryableException, indicating that the operation can be retried
            throw new RetryableException(e);
        }
    }


    /**
     * Retrieves the offset for the given Kafka topic and partition.
     * If the offset is found, it updates the latest timestamp with the value of the offset.
     */
    public String getOffset(String kafkaTopic, String mqttTopic) {
        // Get the OffsetStorageReader from the context
        OffsetStorageReader offsetStorageReader = context.offsetStorageReader();
        // Calculate the partition for the given Kafka topic and MQTT topic
        Map<String, Object> offset = offsetStorageReader.offset(
                getPartition(kafkaTopic, mqttTopic));
        log.info("offset from kafka: {}", offset);
        // If the offset is not null, update the latest timestamp with the offset value
        if (offset != null) {
            return (String) offset.getOrDefault(
                    OFFSET_TIMESTAMP_KEY, ""); // restore offset
        }
        return "";
    }


    /**
     * Retrieves the Kafka topic name.
     * <p>
     * This method extracts the Kafka topic string from the configuration. If multiple topics are configured,
     * it logs a warning and returns only the first topic.
     *
     * @return the Kafka topic name
     */
    public String getKafkaTopic(String kafkaTopics) {
        // Extract the Kafka topics string from the configuration
        String[] split = kafkaTopics.split(",");  // Split by comma
        if (split.length > 1) {
            log.warn("Only one topic is supported. Using the first one: {}", split[0]);
        }
        // Return the first topic
        return split[0];
    }


    /**
     * Overrides the poll method to return a list of SourceRecord objects.
     * This method is used to retrieve the next batch of records from the MQTT source.
     * If the running flag is set to false, it returns null. If there are no records, it returns an empty list.
     *
     * @return a list of SourceRecord objects, or null if the task is not running, or an empty list if there are no records
     * @throws InterruptedException if the thread is interrupted while waiting for records
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // If the running flag is set to false, return null
        if (!running.get()) {
            return null;
        }
        // If the records list is empty, return an empty list
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        // Acquire the lock to ensure thread safety when manipulating the records list
        lock.lock();
        log.info("Returning {} records", records.size());
        // If the records list is empty, release the lock and return an empty list
        if (records.isEmpty()) {
            lock.unlock();
            return Collections.emptyList();
        }

        // Return all the records
        List<SourceRecord> sourceRecords = new ArrayList<>(records);
        records.clear();

        lock.unlock();

        return sourceRecords;
    }


    /**
     * Initializes the MQTT client with the provided configuration.
     * This method is used to create and connect the MQTT client.
     * If an error occurs while initializing the client, a RetryableException is thrown.
     */
    public void initializeMqttClient() {
        try {
            client = MQTTUtil.getMqttClient(config.getMqttConfig());
            MqttConnectOptions mqttConnectOptions = MQTTUtil.defaultOptions(config.getMqttConfig());
            // Connect to the MQTT server using the provided connection options
            client.connect(mqttConnectOptions);
        } catch (Exception e) {
            log.error("Error initializing MQTT client", e);
            throw new RetryableException(e);
        }
    }


    /**
     * Sets the callback for the MQTT client to handle connection loss, message arrival, and delivery completion.
     */
    public void setCallback() {
        client.setCallback(new MqttCallback() {

            /**
             * Handles the event when the connection to the MQTT server is lost.
             * Logs the error and attempts to reconnect using the provided configuration.
             */
            public void connectionLost(Throwable cause) {
                log.error("connectionLost: {}", cause.getMessage(), cause);
                MQTTConfig mqttConfig = config.getMqttConfig();
                synchronized (client) {
                    // double check
                    if (client.isConnected()) {
                        return;
                    }
                    try {
                        tryReconnect(running::get, client, defaultOptions(mqttConfig), mqttConfig);
                    } catch (PanicException exception) {
                        try {
                            client.close(true); // close and disconnect the client
                        } catch (MqttException e) {
                            log.error("Error disconnecting MQTT client", e);
                        }
                    }
                }
            }

            /**
             * Handles the event when a new message arrives from the MQTT server.
             * Logs the received message, processes it, and adds it to the Kafka source records if it is not outdated.
             */
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                log.debug("Received topic[{}] message: {} ", topic, message);

                BridgeMessage bridgeMessage = JsonUtils.fromJson(message.getPayload(), BridgeMessage.class);

                // Check if the received message is older than the latest timestamp.
                if (bridgeMessage.getTimestamp().compareTo(latestTimeStamp) <= 0) {
                    client.messageArrivedComplete(message.getId(), message.getQos());
                    log.warn("Ignore old message. ts:{}  lastTimeStamp:{}", bridgeMessage.getTimestamp(), latestTimeStamp);
                    return; // ignore old messages
                }


                // Create a new Kafka source record from the received MQTT message and add it to the records list.
                lock.lock();
                SourceRecord sourceRecord = new SourceRecord(
                        KafkaUtils.getPartition(kafkaTopic, topic),
                        Collections.singletonMap(OFFSET_TIMESTAMP_KEY, bridgeMessage.getTimestamp()),
                        kafkaTopic, null, bridgeMessage.getContent());
                records.add(sourceRecord);
                mqttIdMap.put(sourceRecord, new Pair<>(message.getId(), message.getQos()));
                lock.unlock();
            }


            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // nop
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


    /**
     * In order to avoid the missing of the mqtt-message, we need to manually acknowledge the mqtt-message.
     * This method is used to acknowledge the receipt of MQTT messages after they have been successfully processed.
     * If the acknowledgment fails, a RetryableException is thrown.
     *
     * @param record   the SourceRecord representing the MQTT message
     * @param metadata the RecordMetadata associated with the record
     * @throws InterruptedException if the thread is interrupted while waiting for the acknowledgment
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        // Call the superclass's commitRecord method (which does nothing in this case)
        super.commitRecord(record, metadata);
        // Get the MQTT message ID and QoS level from the mqttIdMap using the record as a key
        Pair<Integer, Integer> idAndQos = mqttIdMap.get(record);
        try {
            // Complete the message arrival process for the MQTT message with the given ID and QoS level
            client.messageArrivedComplete(idAndQos.getKey(), idAndQos.getValue());
            log.debug("Ack mqtt message with id: {}", idAndQos.getKey());
            log.debug("Success send record: {}", record.value());
        } catch (MqttException e) {
            // Log an error message if the acknowledgment of the MQTT message fails
            log.error("Ack mqtt message failed for record:{}, mqtt-msgId:{}", record, idAndQos.getKey(), e);
            // Throw a RetryableException with a custom error message
            throw new RetryableException("Ack mqtt message failed for record:%s, mqtt-msgId:%s",
                    record.toString(), String.valueOf(idAndQos.getKey()));
        }
    }


}
