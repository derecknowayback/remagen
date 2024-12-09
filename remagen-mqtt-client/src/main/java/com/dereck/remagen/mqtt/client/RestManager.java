package com.dereck.remagen.mqtt.client;


import com.dereck.remagen.mqtt.request.RestfulRequest;
import com.dereck.remagen.mqtt.util.JsonUtils;
import lombok.Data;
import lombok.extern.java.Log;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringRequestContent;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Log
@Data
public class RestManager {

    private boolean needHttps;
    private HttpClient httpClient;
    private String protocolPrefix;

    private String hostAndPort;

    public RestManager(boolean needHttps, String host, String port) {
        this.needHttps = needHttps;
        this.httpClient = new HttpClient();
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
        String json = JsonUtils.toJson(request.getRequestBody());
        try {
            // send request
            ContentResponse resp = httpClient.POST(url).headers(httpFields -> {
                if (request.getRequestHeader() != null) {
                    request.getRequestHeader().forEach(httpFields::put);
                }
            }).body(new StringRequestContent("application/json", json)).send();

            // parse and return response
            return request.parseResp(resp.getContentAsString());
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    public void sendDeleteRequest(String url, Map<String,String> header) {

    }

    public <T> T sendGetRequest(RestfulRequest<T> request) {
        try {
            String url = protocolPrefix + hostAndPort + request.getUri();

            // send request
            ContentResponse response = httpClient.GET(url);

            // parse and return response
            return request.parseResp(response.getContentAsString());
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }




}
