package com.dereck.remagen.mqtt.request.get;

import com.dereck.remagen.mqtt.request.RestfulRequest;
import com.dereck.remagen.mqtt.util.JsonUtils;
import lombok.Data;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.eclipse.jetty.http.HttpMethod;

@Data
public class GetConnectorRequest implements RestfulRequest<ConnectorInfo> {

    private final String name;

    @Override
    public ConnectorInfo parseResp(String rawStr) {
        return JsonUtils.fromJson(rawStr, ConnectorInfo.class);
    }

    @Override
    public String getUri() {
        return String.format("/connector/%s", name);
    }

    @Override
    public HttpMethod getRequestMethod() {
        return HttpMethod.GET;
    }
}
