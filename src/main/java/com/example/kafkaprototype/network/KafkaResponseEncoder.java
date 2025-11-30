package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Encodes Kafka response objects into byte streams.
 * It is currently not executed because
 * KafkaReq hanlder . handleRequest is returning null and ctx.flush isnt executed
 * TODO: return proper response and see if this is returning n/w bytes to socket
 */
@Slf4j
public class KafkaResponseEncoder extends MessageToByteEncoder<KafkaResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, KafkaResponse response, ByteBuf out) throws Exception {
        try {
            // Write correlation ID
            out.writeInt(response.getCorrelationId());
            
            // Write response data
            if (response.getData() != null) {
                out.writeBytes(response.getData());
            }
            
            log.debug("Encoded response with correlation ID: {}", response.getCorrelationId());
            
        } catch (Exception e) {
            log.error("Error encoding response", e);
            throw e;
        }
    }
}
