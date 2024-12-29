package com.dereckchen.remagen.kakfa.restful.client;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class RestManagerTest {

    private RestManager restManagerHttp;
    private RestManager restManagerHttps;
    private RestManager restManagerHttpNoPort;
    private RestManager restManagerHttpsNoPort;

    @Before
    public void setUp() {
        restManagerHttp = new RestManager("localhost", "8080", false);
        restManagerHttps = new RestManager("localhost", "8443", true);
        restManagerHttpNoPort = new RestManager("localhost", null, false);
        restManagerHttpsNoPort = new RestManager("localhost", "", true);
    }

    @Test
    public void getUrl_WithHttpAndPort_ShouldReturnCorrectUrl() {
        String uri = "/test";
        String expectedUrl = "http://localhost:8080/test";
        assertEquals(expectedUrl, restManagerHttp.getUrl(uri));
    }

    @Test
    public void getUrl_WithHttpsAndPort_ShouldReturnCorrectUrl() {
        String uri = "/test";
        String expectedUrl = "https://localhost:8443/test";
        assertEquals(expectedUrl, restManagerHttps.getUrl(uri));
    }

    @Test
    public void getUrl_WithHttpNoPort_ShouldReturnCorrectUrl() {
        String uri = "/test";
        String expectedUrl = "http://localhost/test";
        assertEquals(expectedUrl, restManagerHttpNoPort.getUrl(uri));
    }

    @Test
    public void getUrl_WithHttpsNoPort_ShouldReturnCorrectUrl() {
        String uri = "/test";
        String expectedUrl = "https://localhost/test";
        assertEquals(expectedUrl, restManagerHttpsNoPort.getUrl(uri));
    }
}
