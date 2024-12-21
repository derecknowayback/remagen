package com.dereckchen.remagen.kafka.connector.sink;


import com.dereckchen.remagen.consts.ConnectorConst;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class MqttSinkTaskTest {

    private MqttSinkTask mqttSinkTask;

    @Before
    public void setUp() {
        mqttSinkTask = new MqttSinkTask();
    }

    @Test
    public void version_EnvironmentVariableSet_ReturnsEnvVersion() {
        // Set the environment variable
        System.setProperty("REMAGEN.VERSION", "1.2.3");

        // Call the method under test
        String version = mqttSinkTask.version();

        // Verify the result
        assertEquals("1.2.3", version);
    }

    @Test
    public void version_EnvironmentVariableNotSet_ReturnsDefaultVersion() {
        // Unset the environment variable
        System.clearProperty("REMAGEN.VERSION");

        // Call the method under test
        String version = mqttSinkTask.version();

        // Verify the result
        assertEquals(ConnectorConst.DEFAULT_VERSION, version);
    }
}
