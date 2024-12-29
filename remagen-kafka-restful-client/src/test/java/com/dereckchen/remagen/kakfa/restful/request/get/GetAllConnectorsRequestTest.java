package com.dereckchen.remagen.kakfa.restful.request.get;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class GetAllConnectorsRequestTest {

    private GetAllConnectorsRequest request;

    @Before
    public void setUp() {
        request = new GetAllConnectorsRequest();
    }

    @Test
    public void parseResp_ValidJsonString_ReturnsList() {
        String validJson = "[\"connector1\", \"connector2\", \"connector3\"]";
        List<String> expectedList = Arrays.asList("connector1", "connector2", "connector3");
        List<String> result = request.parseResp(validJson);
        assertEquals(expectedList, result);
    }

    @Test
    public void parseResp_InvalidJsonString_ReturnsNull() {
        String invalidJson = "{\"key\": \"value\"}"; // Not a list of strings
        List<String> result = request.parseResp(invalidJson);
        assertNull(result);
    }

    @Test
    public void parseResp_EmptyJsonString_ReturnsEmptyList() {
        String emptyJson = "[]";
        List<String> expectedList = Collections.emptyList();
        List<String> result = request.parseResp(emptyJson);
        assertEquals(expectedList, result);
    }
}
