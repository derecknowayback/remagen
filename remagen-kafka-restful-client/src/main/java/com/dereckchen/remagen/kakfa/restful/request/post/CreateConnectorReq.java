package com.dereckchen.remagen.kakfa.restful.request.post;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.utils.JsonUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.eclipse.jetty.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

public class CreateConnectorReq extends CreateConnectorRequest implements RestfulRequest<ConnectorInfo> {

    public CreateConnectorReq(String name, Map<String, String> config, InitialState initialState) {
        super(name, config, initialState);
    }

    @Override
    public ConnectorInfo parseResp(String rawStr) {
        return JsonUtils.fromJson(rawStr, ConnectorInfo.class);
    }

    @Override
    public String getUri() {
        return "/connectors";
    }

    @Override
    public HttpMethod getRequestMethod() {
        return HttpMethod.POST;
    }

    @Override
    public Object getRequestBody() {
        return new HashMap<String, Object>() {
            {
                put("name", name());
                put("config", config());
                put("initial_state", initialState());
            }
        };
    }
}
