package com.dereckchen.remagen.kafka.connector.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dereckchen.remagen.consts.ConnectorConst.*;

@Slf4j
public class MqttSinkConnector extends SinkConnector {

    private Map<String, String> configProps;
    private String name;

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // for all tasks, we use same configuration
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
        name = props.get(PROPS_CONNECTOR_NAME);
        log.info("Starting MqttSinkConnector with name: {}", name);
    }


    @Override
    public void stop() {
        log.info("MqttSinkConnector {} stopped ...", name);
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(PROPS_MQTT_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client password")
                .define(PROPS_MQTT_CLIENTID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client id")
                .define(PROPS_MQTT_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client username")
                .define(PROPS_MQTT_BROKER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt broker")
                .define(PROPS_MQTT_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt topic")
                .define(PROPS_CONNECTOR_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "connector name")
                .define(PROPS_CONNECTOR_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "connector class")
                .define(PROPS_TASKS_MAX, ConfigDef.Type.INT, 1, ConfigDef.Importance.LOW, "tasks max")
                .define(PROPS_TASKS_SINK_TOPICS, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "topics");
        return configDef;
    }

    @Override
    public String version() {
        // get version from env
        String VERSION = System.getenv(VERSION_ENV_KEY);
        if (VERSION == null) {
            VERSION = DEFAULT_VERSION;
        }
        log.info("MqttSinkConnector version: {}", VERSION);
        return VERSION;
    }
}
