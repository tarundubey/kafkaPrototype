package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Default implementation of KafkaResponse that holds raw response data.
 * Similar to DefaultKafkaRequest except it has no api version and clientId
 * TODO and NOTE: atm we are returning null from Req handler - to be worked on later
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DefaultKafkaResponse extends KafkaResponse {
    private final ByteBuf data;
    
    public DefaultKafkaResponse(short apiKey, int correlationId, ByteBuf data) {
        super(apiKey, correlationId);
        this.data = data != null ? data : Unpooled.EMPTY_BUFFER;
    }
    
    @Override
    public ByteBuf getData() {
        return data.retain();
    }
    
    @Override
    public void release() {
        if (data != null && data.refCnt() > 0) {
            data.release();
        }
    }
}
