package com.dereckchen.remagen.kafka.connector.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MqttSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
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
    }


    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        configDef.define("mqtt.password", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client password")
                .define("mqtt.clientid", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client id")
                .define("mqtt.username", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt client username")
                .define("mqtt.broker", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt broker")
                .define("mqtt.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mqtt topic")
                .define("name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "connector name")
                .define("connector.class", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "connector class")
                .define("tasks.max", ConfigDef.Type.INT, 1, ConfigDef.Importance.LOW, "tasks max")
                .define("topics", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "topics");
        return configDef;

    }

    @Override
    public String version() {
        return "";
    }
}
