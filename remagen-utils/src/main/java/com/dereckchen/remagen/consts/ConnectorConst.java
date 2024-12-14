package com.dereckchen.remagen.consts;

public class ConnectorConst {

    public static final String CONNECTOR_NAME_FORMAT = "mqtt-%s-kafka-%s";

    // MQTT
    public static final int MQTT_MAX_CONNECT_RETRY_COUNT = 10;
    public static final int MQTT_RECONNECT_PERIOD = 1000;
    public static final String PROPS_MQTT_PASSWORD = "mqtt.password";
    public static final String PROPS_MQTT_CLIENTID = "mqtt.clientid";
    public static final String PROPS_MQTT_USERNAME = "mqtt.username";
    public static final String PROPS_MQTT_BROKER = "mqtt.broker";
    public static final String PROPS_MQTT_TOPIC = "mqtt.topic";

    // KAFKA
    public static final String PROPS_KAFKA_HOST = "kafka.host";
    public static final String PROPS_KAFKA_PORT = "kafka.port";
    public static final String PROPS_KAFKA_NEED_HTTPS = "kafka.needHttps";
    public static final String PROPS_KAFKA_TOPIC = "kafka.topic";


    public static final String KAFKA_PROP_KEY = "topic";

    // Kafka message partition
    public static final String PARTITION_KAFKA_TOPIC_KEY = "kafkaTopic";
    public static final String PARTITION_MQTT_TOPIC_KEY = "mqttTopic";

    // Kafka offset manage
    public static final String OFFSET_TIMESTAMP_KEY = "timestamp";


}
