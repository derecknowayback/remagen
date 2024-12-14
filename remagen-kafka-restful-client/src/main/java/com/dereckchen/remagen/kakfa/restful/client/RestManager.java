package com.dereckchen.remagen.kakfa.restful.client;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@Data
public class RestManager {

    private boolean needHttps;
    private HttpClient httpClient;
    private String protocolPrefix;

    private String hostAndPort;

    public RestManager(String host, String port, boolean needHttps) {
        this.needHttps = needHttps;
        this.httpClient = new HttpClient();
        try {
            httpClient.start();
        } catch (Exception e) {
            log.error("HttpClient start error", e);
            throw new RuntimeException(e);
        }
        if (needHttps) {
            protocolPrefix = "https://";
        } else {
            protocolPrefix = "http://";
        }

        if (port == null || port.isEmpty()) {
            this.hostAndPort = host; // just host, resolved by dns
        } else {
            this.hostAndPort = host + ":" + port;
        }
    }


    public <T> T sendPostRequest(RestfulRequest<T> request) {
        String url = protocolPrefix + hostAndPort + request.getUri();
        String json = JsonUtils.toJsonString(request.getRequestBody());
        try {
            // send request
            Request post = httpClient.POST(url);
            Map<String, String> header = request.getRequestHeader();
            if (header != null && !header.isEmpty()) {
                header.forEach(post::header);
            }
            ContentResponse resp = post.content(new StringContentProvider(
                    "application/json", json, StandardCharsets.UTF_8)).send();

            // parse and return response
            return request.parseResp(resp.getContentAsString());
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T sendGetRequest(RestfulRequest<T> request) {
        try {
            String url = protocolPrefix + hostAndPort + request.getUri();

            // send request
            ContentResponse response = httpClient.GET(url);

            // parse and return response
            return request.parseResp(response.getContentAsString());
        } catch (Exception e) {
            log.error("Get request error: {}", request, e);
            throw new RuntimeException(e);
        }
    }


    public <T> T sendDeleteRequest(RestfulRequest<T> request) {
        try {
            String url = protocolPrefix + hostAndPort + request.getUri();
            Request delete = httpClient.newRequest(url).method(HttpMethod.DELETE);

            Map<String, String> header = request.getRequestHeader();
            if (header != null && !header.isEmpty()) {
                header.forEach(delete::header);
            }

            ContentResponse resp = delete.send();

            return request.parseResp(resp.getContentAsString());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Delete request error: {}", request, e);
            throw new RuntimeException(e);
        }
    }
}
