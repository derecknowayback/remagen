package com.dereckchen.remagen.consts;

public class ConnectorConst {

    public static final String VERSION_ENV_KEY = "REMAGEN.VERSION";

    public static final String CONNECTOR_NAME_FORMAT = "mqtt-%s-kafka-%s";

    public static final String DEFAULT_VERSION = "0.1-BETA";

    // CONNECTOR
    public static final String PROPS_CONNECTOR_NAME = "name";
    public static final String PROPS_CONNECTOR_CLASS = "connector.class";
    public static final String PROPS_TASKS_MAX = "tasks.max";
    public static final String PROPS_TASKS_SINK_TOPICS = "topics";
    public static final String PROPS_TASKS_SOURCE_TOPIC = "topic";

    // MQTT
    public static final int MQTT_QOS = 1;
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

    public static final String INTERCEPTOR_PROP_HOST = "kafkaConnectManager.host";
    public static final String INTERCEPTOR_PROP_PORT = "kafkaConnectManager.port";

    // Kafka message partition
    public static final String PARTITION_KAFKA_TOPIC_KEY = "kafkaTopic";
    public static final String PARTITION_MQTT_TOPIC_KEY = "mqttTopic";

    // Kafka offset manage
    public static final String OFFSET_TIMESTAMP_KEY = "timestamp";

    // metrics
    public static final String PUSH_GATE_WAY_ENV = "PUSH_GATE_WAY_URL";
    public static final String SOURCE_TASK_METRICS = "source_task_metrics";
    public static final String SINK_TASK_METRICS = "sink_task_metrics";
    public static final int PUSH_GATE_WAY_INTERVAL = 1 * 1000;
}
