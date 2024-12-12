package com.dereckchen.remagen.kakfa.restful.request.delete;

import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpMethod;

@Data
@Slf4j
public class DeleteConnectorRequest implements RestfulRequest<Void> {
    private String connectorName;

    private static final String DELETE_CONNECTOR_URI_PATTERN = "/connectors/%s";

    public DeleteConnectorRequest(String connectorName) {
        this.connectorName = connectorName;
    }


    @Override
    public Void parseResp(String rawStr) {
        return null;
    }

    @Override
    public String getUri() {
        return String.format(DELETE_CONNECTOR_URI_PATTERN, connectorName);
    }

    @Override
    public HttpMethod getRequestMethod() {
        return HttpMethod.DELETE;
    }
}
