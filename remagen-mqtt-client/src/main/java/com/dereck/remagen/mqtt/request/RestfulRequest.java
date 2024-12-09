package com.dereck.remagen.mqtt.request;

import org.eclipse.jetty.http.HttpMethod;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface RestfulRequest<Resp> {

    Map<String,String> DEFAULT_HEADER = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("Content-Type", "application/json");
                put("Accept", "application/json");
            }}
    );

    Resp parseResp(String rawStr);

    String getUri();

    HttpMethod getRequestMethod();

    default Object getRequestBody() {
        return null;
    }

    default Map<String,String> getRequestHeader() {
        return RestfulRequest.DEFAULT_HEADER;
    }
}
