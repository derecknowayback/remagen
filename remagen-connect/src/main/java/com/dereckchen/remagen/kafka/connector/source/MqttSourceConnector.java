package com.dereckchen.remagen.kafka.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class MqttSourceConnector extends SourceConnector {

    private Map<String,String> props;

    @Override
    public void start(Map<String, String> map) {
        this.props = map;
        log.info("Starting MqttSourceConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return Collections.singletonList(props);
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
                .define("topic", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "topic");
        return configDef;
    }

    @Override
    public String version() {
        return "";
    }
}
