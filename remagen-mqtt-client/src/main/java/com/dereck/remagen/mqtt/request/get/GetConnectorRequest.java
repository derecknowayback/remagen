package com.dereck.remagen.mqtt.request.get;

import com.dereck.remagen.mqtt.request.RestfulRequest;
import com.dereck.remagen.mqtt.util.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.eclipse.jetty.http.HttpMethod;

@Data
@Slf4j
@AllArgsConstructor
public class GetConnectorRequest implements RestfulRequest<ConnectorInfo> {

    private static final String GET_CONNECTOR_URI_PATTERN = "/connectors/%s";
    private String connectorName;

    @Override
    public ConnectorInfo parseResp(String rawStr) {
        try {
            return JsonUtils.fromJson(rawStr, ConnectorInfo.class);
        } catch (Exception e) {
            log.error("GetConnectorRequest parseResp error", e);
            return null;
        }
    }

    @Override
    public String getUri() {
        return String.format(GET_CONNECTOR_URI_PATTERN, connectorName);
    }

    @Override
    public HttpMethod getRequestMethod() {
        return HttpMethod.GET;
    }
}
