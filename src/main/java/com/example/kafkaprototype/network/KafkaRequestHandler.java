package com.example.kafkaprototype.network;

import com.example.kafkaprototype.broker.KafkaBroker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles incoming Kafka protocol requests
 * Netty abstract class that simplifies the processing of inbound messages in a type-safe manner.
 * Automatically releases message objects after processing
 * Checks for type (Generic )
 * Releases message objects after processing
 * Single method to implement for message handling called channelRead0 and manages ref counting
 * When a new connection is accepted, Netty:
 * Creates a new Channel instance
 * Creates a DefaultChannelPipeline for the channel
 * For each handler added via addLast():
 * Creates a new DefaultChannelHandlerContext
 * Links it into the pipeline
 * Associates it with the handler
 * This is how this handler is initialized
 */
@Slf4j
public class KafkaRequestHandler extends SimpleChannelInboundHandler<KafkaRequest> {
    private final KafkaBroker broker;

    public KafkaRequestHandler(KafkaBroker broker) {
        this.broker = broker;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KafkaRequest request) {
        try {
            //ChannelHandlerContext is an important component
            //Entry point to the pipeline
            //Can be used to dynamically modify the pipeline
            KafkaResponse response = handleRequest(request);
            if (response != null) {
                ctx.writeAndFlush(response);
                // this response travels backward to Kafka response encoder
                // and then to NetworkServer socket
            }
        } catch (Exception e) {
            log.error("Error processing request: {}", request, e);
            // In a real implementation, we would send an error response
        }
    }

    private KafkaResponse handleRequest(KafkaRequest request) {
        // In a real implementation, we would handle different types of requests
        // For now, we'll just log the request and return a simple response
        log.debug("Received request: {}", request);

        // TODO: return proper response
        
        // This is a simplified implementation
        // In a real Kafka broker, we would have proper request/response handling
        // based on the API key and version
        
        return null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in channel handler", cause);
        ctx.close();
    }
}
