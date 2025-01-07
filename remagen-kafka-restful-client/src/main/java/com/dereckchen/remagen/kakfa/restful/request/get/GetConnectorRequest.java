package com.dereckchen.remagen.kakfa.restful.request.get;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.models.ConnectorInfoV2;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.eclipse.jetty.http.HttpMethod;

@Data
@Slf4j
@AllArgsConstructor
public class GetConnectorRequest implements RestfulRequest<ConnectorInfoV2> {

    private static final String GET_CONNECTOR_URI_PATTERN = "/connectors/%s";
    private String connectorName;

    @Override
    public ConnectorInfoV2 parseResp(String rawStr) {
        try {
            return JsonUtils.fromJson(rawStr, ConnectorInfoV2.class);
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
