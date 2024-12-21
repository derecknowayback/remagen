package com.dereckchen.remagen.utils;


import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class JsonUtilsTest {

    private List<String> stringList;

    @Before
    public void setUp() {
        stringList = new ArrayList<>();
        stringList.add("test");
    }

    @Test
    public void toJsonString_ValidObject_ReturnsJsonString() {
        String jsonString = JsonUtils.toJsonString(stringList);
        assertEquals("[\"test\"]", jsonString);
    }

    @Test
    public void toJsonBytes_ValidObject_ReturnsJsonBytes() {
        byte[] jsonBytes = JsonUtils.toJsonBytes(stringList);
        assertEquals("[\"test\"]", new String(jsonBytes));
    }

    @Test
    public void fromJson_ValidJsonString_ReturnsObject() {
        String jsonString = "[\"test\"]";
        List<String> result = JsonUtils.fromJson(jsonString, new TypeReference<List<String>>() {});
        assertEquals(stringList, result);
    }

    @Test
    public void toJsonString_ExceptionThrown_ThrowsRuntimeException() {
        Object invalidObject = new Object() {
            private void writeObject(java.io.ObjectOutputStream stream) throws java.io.NotSerializableException {
                throw new java.io.NotSerializableException("Serialization error");
            }
        };

        assertThrows(RuntimeException.class, () -> JsonUtils.toJsonString(invalidObject));
    }

    @Test
    public void toJsonBytes_ExceptionThrown_ThrowsRuntimeException() {
        Object invalidObject = new Object() {
            private void writeObject(java.io.ObjectOutputStream stream) throws java.io.NotSerializableException {
                throw new java.io.NotSerializableException("Serialization error");
            }
        };

        assertThrows(RuntimeException.class, () -> JsonUtils.toJsonBytes(invalidObject));
    }

    @Test
    public void fromJson_InvalidJsonString_ThrowsRuntimeException() {
        String invalidJson = "[\"test\"";
        assertThrows(RuntimeException.class, () -> JsonUtils.fromJson(invalidJson, new TypeReference<List<String>>() {}));
    }
}
