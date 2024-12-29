package com.dereckchen.remagen.kakfa.restful.request.get;


import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetConnectorRequestTest {

    private GetConnectorRequest request;

    @Before
    public void setUp() {
        request = new GetConnectorRequest("testConnector");
    }

    @Test
    public void parseResp_ValidJsonString_ReturnsConnectorInfo() {
        String validJson = "{\"name\":\"testConnector\",\"config\":{\"key\":\"value\"},\"tasks\":[],\"type\":\"source\"}";
        ConnectorInfo expectedInfo = new ConnectorInfo("testConnector",
                Collections.singletonMap("key", "value"), Collections.emptyList(), ConnectorType.SOURCE);

        ConnectorInfo result = request.parseResp(validJson);

        assertEquals(expectedInfo, result);
    }

    @Test
    public void parseResp_InvalidJsonString_ReturnsNull() {
        String invalidJson = "invalid-json-string";

        ConnectorInfo result = request.parseResp(invalidJson);

        assertNull(result);
    }
}
