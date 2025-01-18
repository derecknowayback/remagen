package com.dereckchen.remagen.kafka.connector.source;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.dereckchen.remagen.consts.ConnectorConst.VERSION_ENV_KEY;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MqttSourceConnector.class, System.class})
public class MqttSourceConnectorTest {

    private MqttSourceConnector connector;

    @Before
    public void setUp() {
        connector = new MqttSourceConnector();
        mockStatic(System.class);
    }

    @Test
    public void version_EnvironmentVariableSet_ReturnsEnvVersion() {
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
        assertEquals("0.1-BETA", version); // Assuming DEFAULT_VERSION is "0.0.1"
    }


}
