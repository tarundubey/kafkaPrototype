package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;
import lombok.Data;

/**
 * Base class for all Kafka protocol requests including fetch get metadata
 * TODO evaluate interface for this maybe
 * Again - keep things private
 */
@Data
public abstract class KafkaRequest {
    private final int apiKey; //Identifies the type of request (exp: produce, fetch, metadata)
    private final int apiVersion; // Version of the API being used
    private final int correlationId; // identifier for matching requests and responses
    private final String clientId; // Identifier for the client making the request
    
    public abstract ByteBuf getData();
    
    public void release() {
        // Default implementation does nothing
        // implemented in child classes
    }
}
