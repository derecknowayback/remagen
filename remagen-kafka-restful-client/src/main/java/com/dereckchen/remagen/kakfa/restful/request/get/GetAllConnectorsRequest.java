package com.dereckchen.remagen.kakfa.restful.request.get;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.utils.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpMethod;

import java.util.List;

@Data
@Slf4j
public class GetAllConnectorsRequest implements RestfulRequest<List<String>> {

    private final static TypeReference<List<String>> STRING_LIST_TYPE_REF = new TypeReference<List<String>>() {
    };

    @Override
    public List<String> parseResp(String rawStr) {
        try {
            return JsonUtils.fromJson(rawStr, STRING_LIST_TYPE_REF);
        } catch (Exception e) {
            log.error("GetAllConnectorsRequest parseResp error", e);
            return null;
        }
    }

    @Override
    public String getUri() {
        return "/connectors";
    }

    @Override
    public HttpMethod getRequestMethod() {
        return HttpMethod.GET;
    }
}
