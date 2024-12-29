package com.dereckchen.remagen.kafka.connector.sink;

import com.dereckchen.remagen.consts.ConnectorConst;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static com.dereckchen.remagen.consts.ConnectorConst.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({System.class, MqttSinkConnector.class})
public class MqttSinkConnectorTest {

    @InjectMocks
    private MqttSinkConnector connector;


    private Map<String, String> configProps;


    @Before
    public void setUp() {
        configProps = new HashMap<>();
        configProps.put("key1", "value1");
        configProps.put("key2", "value2");
        connector.start(configProps);
    }

    @Test
    public void taskConfigs_MaxTasksZero_ReturnsEmptyList() {
        List<Map<String, String>> configs = connector.taskConfigs(0);
        assertTrue(configs.isEmpty());
    }

    @Test
    public void taskConfigs_MaxTasksOne_ReturnsSingleConfig() {
        List<Map<String, String>> configs = connector.taskConfigs(1);
        assertEquals(1, configs.size());
        assertEquals(configProps, configs.get(0));
    }

    @Test
    public void taskConfigs_MaxTasksMultiple_ReturnsMultipleConfigs() {
        int maxTasks = 3;
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        assertEquals(maxTasks, configs.size());
        for (Map<String, String> config : configs) {
            assertEquals(configProps, config);
        }
    }

    @Test
    public void config_ShouldDefineAllExpectedProperties() {
        ConfigDef configDef = connector.config();

        // Verify that all expected properties are defined
        assertTrue(configDef.configKeys().containsKey(PROPS_MQTT_PASSWORD));
        assertTrue(configDef.configKeys().containsKey(PROPS_MQTT_CLIENTID));
        assertTrue(configDef.configKeys().containsKey(PROPS_MQTT_USERNAME));
        assertTrue(configDef.configKeys().containsKey(PROPS_MQTT_BROKER));
        assertTrue(configDef.configKeys().containsKey(PROPS_MQTT_TOPIC));
        assertTrue(configDef.configKeys().containsKey(PROPS_CONNECTOR_NAME));
        assertTrue(configDef.configKeys().containsKey(PROPS_CONNECTOR_CLASS));
        assertTrue(configDef.configKeys().containsKey(PROPS_TASKS_MAX));
        assertTrue(configDef.configKeys().containsKey(PROPS_TASKS_SINK_TOPICS));

        // Verify the type and default value of a specific property
        ConfigDef.ConfigKey tasksMaxKey = configDef.configKeys().get(PROPS_TASKS_MAX);
        assertEquals(ConfigDef.Type.INT, tasksMaxKey.type);
        assertEquals(1, tasksMaxKey.defaultValue);
    }

    @Test
    public void version_EnvironmentVariableSet_ReturnsEnvVersion() {
        mockStatic(System.class);
        when(System.getenv(VERSION_ENV_KEY)).thenReturn("1.2.3");

        // Call the method under test
        String version = connector.version();

        // Verify the result
        assertEquals("1.2.3", version);
    }

    @Test
    public void version_EnvironmentVariableNotSet_ReturnsDefaultVersion() {
        // Call the method under test
        String version = connector.version();

        // Verify the result
        assertEquals(ConnectorConst.DEFAULT_VERSION, version);
    }
}
