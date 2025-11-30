package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;

/**
 * Default implementation of KafkaRequest that holds raw request data.
 */
@Getter
public class DefaultKafkaRequest extends KafkaRequest {
    private final ByteBuf data;
    // Unpooled buffer is a special constant in Netty that represents an empty, read-only buffer
    //In Netty's ByteBuf,  Refernce counting is a memory management
    // technique that's crucial for preventing memory leaks in network applications
    // A technique where an object keeps track of how many references point to it
    //When the count reaches zero, the object can be safely deallocated
    //retain(): Increments the reference count
    //release(): Decrements the reference count
    //refCnt(): Returns the current reference count
    
    public DefaultKafkaRequest(int apiKey, int apiVersion, int correlationId, String clientId, ByteBuf data) {
        super(apiKey, apiVersion, correlationId, clientId);
        this.data = data != null ? data : Unpooled.EMPTY_BUFFER;
    }
    
    @Override
    public ByteBuf getData() {
        return data.retain(); // Increments reference count centrally
    }
    
    public void release() {
        if (data != null && data.refCnt() > 0) {
            data.release(); // Decrements reference count -
        }
    }
}
