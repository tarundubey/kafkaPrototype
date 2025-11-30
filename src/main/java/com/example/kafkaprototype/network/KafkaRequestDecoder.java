package com.example.kafkaprototype.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Decodes incoming raw n/w byte streams into Kafka request objects. extends Nettys ByteToMessageDecoder
 * Channel Handler context from netty Acts as a bridge between handlers in the pipeline, providing:
 * Access to the Channel (connection)
 * Methods to trigger events in the pipeline
 * Access to the ChannelHandler that this context is associated with
 * Methods to modify the pipeline dynamically
 * Now talking about ByteToMessageDecoder -Handles the accumulation of bytes until a complete message is received
 * Manages the complexity of TCP's streaming nature (where messages can be split or combined)
 * Provides a simple decode() method to implement
 * All these are provided by netty and the complexity to handle TCP complexities reduce because of these classes
 */
@Slf4j
public class KafkaRequestDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Ensure we have enough bytes for the request header (API key + API version + correlation ID + client ID length)
        if (in.readableBytes() < 14) {
            return; // wait for more data
        }

        // Mark the current read position in case we need to reset due to an error
        in.markReaderIndex();

        // this is more like saving bookmarks
        //TCP packets can be split or combined for a single message
        // if we move forward and try to read more data we might end up reading wrong data
        // so we mark the current read position and reset it if we need to
        try {
            // Read request header
            short apiKey = in.readShort();
            short apiVersion = in.readShort();
            int correlationId = in.readInt();
            String clientId = readString(in);
            
            // Create a new request object using the concrete DefaultKafkaRequest
            KafkaRequest request = new DefaultKafkaRequest(apiKey, apiVersion, correlationId, clientId, in.retain());
            out.add(request);
            
        } catch (Exception e) {
            log.error("Error decoding request", e);
            in.resetReaderIndex();
            throw e;
        }
    }
    
    private String readString(ByteBuf in) {
        short length = in.readShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readBytes(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
