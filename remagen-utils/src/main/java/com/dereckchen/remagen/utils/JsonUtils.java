package com.dereckchen.remagen.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.registerModule(new JavaTimeModule());
    }

    public static String toJsonString(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            log.error("JsonUtils toJsonString error", e);
            throw new RuntimeException("JsonUtils toJsonString error", e);
        }
    }

    public static byte[] toJsonBytes(Object object) {
        try {
            return mapper.writeValueAsBytes(object);
        } catch (Exception e) {
            log.error("JsonUtils toJsonString error", e);
            throw new RuntimeException("JsonUtils toJsonString error", e);
        }
    }


    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("JsonUtils fromJson error", e);
            throw new RuntimeException("JsonUtils fromJson error", e);
        }
    }

    public static <T> T fromJson(byte[] json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("JsonUtils fromJson error", e);
            throw new RuntimeException("JsonUtils fromJson error", e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<T> ref) {
        try {
            return mapper.readValue(json, ref);
        } catch (Exception e) {
            log.error("JsonUtils fromJson error", e);
            throw new RuntimeException("JsonUtils fromJson error", e);
        }
    }
}
