package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;

/**
 * Base class for all Kafka protocol responses
 */
public abstract class KafkaResponse {
    private final short apiKey;
    private final int correlationId;

    protected KafkaResponse(short apiKey, int correlationId) {
        this.apiKey = apiKey;
        this.correlationId = correlationId;
    }

    public short getApiKey() {
        return apiKey;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    /**
     * Get the response data as a ByteBuf
     */
    public abstract ByteBuf getData();

    /**
     * Release any resources held by this response
     */
    public abstract void release();
}
