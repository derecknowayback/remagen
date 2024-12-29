package com.dereckchen.remagen.kafka.connector.sink;


import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.models.MQTTConfig;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.KafkaUtils;
import com.dereckchen.remagen.utils.MQTTUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dereckchen.remagen.consts.ConnectorConst.DEFAULT_VERSION;
import static com.dereckchen.remagen.consts.ConnectorConst.VERSION_ENV_KEY;

@Slf4j
public class MqttSinkTask extends SinkTask {


    private MqttClient mqttClient;
    private MQTTConfig mqttConfig;
    private MqttConnectOptions mqttConnectOptions;
    private AtomicBoolean running = new AtomicBoolean(false);
    private Set<String> kafkaTopics;

    @Override
    public String version() {
        // get version from env
        String VERSION = System.getenv(VERSION_ENV_KEY);
        if (VERSION == null) {
            VERSION = DEFAULT_VERSION;
        }
        log.info("MqttSinkTask version: {}", VERSION);
        return VERSION;
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("MqttSinkTask start... Using props: {}", props);

        // parse mqtt config
        parseConfig(props);

        // init running status var
        running.set(true);
    }

    /**
     * Parse the configuration information from the properties map
     *
     * @param props A map containing the configuration properties
     */
    public void parseConfig(Map<String, String> props) {
        // Parse the MQTT configuration from the properties map
        mqttConfig = MQTTUtil.parseConfig(props);
        log.info("Use mqttConfig: {}", mqttConfig);

        // Parse the Kafka server configuration from the properties map
        KafkaServerConfig kafkaServerConfig = KafkaUtils.parseConfig(props);
        // Retrieve the Kafka topics from the Kafka server configuration
        String kafkaTopicsStr = kafkaServerConfig.getKafkaTopics();
        // Split the Kafka topics string into a set of topics
        kafkaTopics = new HashSet<>(Arrays.asList(kafkaTopicsStr.split(",")));
        log.info("Use kafkaTopics: {}", kafkaTopics);
    }


    /**
     * Overrides the put method to process a collection of SinkRecords
     * This method first checks if the SinkTask is running, and if not, it simply returns without processing
     * If it is running, it starts processing the records collection, logging relevant information during the process
     *
     * @param records A collection of SinkRecord objects, representing the records to be processed
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        // if it is not running, do nothing, just return
        if (isRunning()) {
            log.warn("SinkTask is not running...");
            return;
        }

        log.info("Start polling record from Kafka...");

        // If the polled records are null or empty, log the information and return
        if (records == null || records.isEmpty()) {
            log.info("Polled empty records from Kafka.");
            return;
        }

        // Monitor the records
        monitorRecords(records);

        // Send the records to MQTT
        try {
            sendMQTT(records);
        } catch (PanicException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Monitors and logs information about the received records.
     * <p>
     * This method checks if the provided collection of records is null or empty.
     * If it contains elements, it retrieves the first record to log details such as
     * the number of records received and the Kafka coordinates (topic, partition, offset)
     * of the first record. It logs this information for monitoring purposes.
     *
     * @param records A collection of {@code SinkRecord} objects representing the records to be monitored.
     */
    public void monitorRecords(Collection<SinkRecord> records) {
        // Return early if the records collection is null or empty
        if (records == null || records.isEmpty()) {
            return;
        }

        // Retrieve the first record and the total count of records for logging
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();

        // Log the number of received records and the Kafka coordinates of the first record
        log.info(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
    }


    /**
     * Send a collection of records via MQTT
     * This method iterates through each SinkRecord in the collection, converts it to a JSON byte array, and sends it via MQTT
     * If the MQTT client has not been initialized, it will be initialized; if the client is disconnected, it will attempt to reconnect
     *
     * @param records A collection of SinkRecord objects, representing the records to be sent via MQTT
     */
    public void sendMQTT(Collection<SinkRecord> records) throws PanicException {
        for (SinkRecord record : records) {
            try {
                // Retrieve the value of the record, which will be converted to a JSON byte array and encapsulated into an MQTT message
                Object obj = record.value();
                MqttMessage mqttMessage = new MqttMessage(JsonUtils.toJsonBytes(obj));
                // if we didn't init mqtt client, init it
                if (mqttClient == null) {
                    initMqttClient();
                }
                if (!mqttClient.isConnected()) {
                    // if mqtt client is not connected, try reconnect
                    MQTTUtil.tryReconnect(running::get, mqttClient, mqttConnectOptions, mqttConfig);
                }
                mqttClient.publish(mqttConfig.getTopic(), mqttMessage);
            } catch (MqttException e) {
                log.error("Send mqtt message error", e);
                throw new RetryableException(e);
            }
        }
    }

    /**
     * Initializes the MQTT client.
     * This method creates and initializes an MQTT client based on the provided configuration,
     * establishing a connection to the MQTT broker. It retrieves an MQTT client instance and
     * default connection options via the MQTT utility class, then attempts to connect to the broker.
     * If a connection error occurs, it logs the error and throws a retryable exception for upper-level handling.
     */
    public void initMqttClient() {
        // Obtain the MQTT client instance
        mqttClient = MQTTUtil.getMqttClient(mqttConfig);

        // Retrieve default MQTT connection options
        mqttConnectOptions = MQTTUtil.defaultOptions(mqttConfig);

        try {
            // Attempt to connect to the MQTT broker using the default options
            mqttClient.connect(mqttConnectOptions);
        } catch (MqttException e) {
            log.error("Connect to mqtt broker error", e);

            // Throw a retryable exception to indicate that the connection may need to be retried
            throw new RetryableException(e);
        }
    }


    /**
     * Stops the current running MQTT client and releases resources.
     * <p>
     * This method is used to properly close the MQTT client connection and release the resources it is using.
     * It logs the status of resource release for monitoring and troubleshooting purposes.
     */
    @Override
    public void stop() {
        log.info("Closing resources...");
        try {
            // Attempt to close the MQTT client connection
            mqttClient.close();
            // Set the running status to false, after mqttClient stop
            running.set(false);
            log.info("Successfully closed mqtt client...");
        } catch (MqttException e) {
            // If an exception occurs during the closure process, log the error details
            log.error("Close mqttClient error", e);
            throw new RuntimeException(e);
        }
    }
}
