package com.dereckchen.remagen.models;

public interface IBridgeMessageContent {
    String serializeToJsonStr();

    String getMessageId();
}
