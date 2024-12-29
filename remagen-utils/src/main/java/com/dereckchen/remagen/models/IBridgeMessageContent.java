package com.dereckchen.remagen.models;

public interface IBridgeMessageContent {
    /**
     * Serializes the object to a JSON string.
     *
     * @return A JSON string representation of the object.
     */
    String serializeToJsonStr();


    /**
     * Retrieves the unique identifier for the message.
     *
     * @return A string representing the message ID.
     */
    String getMessageId();

}
