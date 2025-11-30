package com.example.kafkaprototype.network;

import com.example.kafkaprototype.broker.KafkaBroker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * Network server that handles client connections using Netty.
 *it's per broker - Each NetworkServer instance serves a single broker
 * Each broker in a Kafka cluster runs its own NetworkServer
 * The port number in the constructor determines which port the broker listens on
 *
 */
@Slf4j
public class NetworkServer {
    private final KafkaBroker broker;
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    // server channel: A "promise" that will be completed when a network operation finishes
    private ChannelFuture serverChannel;

    public NetworkServer(KafkaBroker broker, int port) {
        this.broker = broker;
        this.port = port;
    }

    /**
     * Start the network server
     */
    public void start() {
        bossGroup = new NioEventLoopGroup(1); // main thread group for handling connections only
        workerGroup = new NioEventLoopGroup(); // worker threads for handling I/O operations
        
        try {
            //Using netty ServerBootstrap to bootstrap the server with ports and config
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    // Uses Java NIO for non blocking concurrent connections
             .channel(NioServerSocketChannel.class)
                    // child handler is the handler for each connection
                    // more of a working manual for each worker thread
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     // Add handlers for:
                     // 1. Length-based frame decoder
                     // 2. Request handler
                     // 3. Exception handler
                     p.addLast(
                         new KafkaRequestDecoder(), // Inbound
                         new KafkaResponseEncoder(), // Outbound
                         new KafkaRequestHandler(broker) // Business Logic
                     );
                 }
             })
                    // Sets the maximum number of pending connections in the accept queue
             .option(ChannelOption.SO_BACKLOG, 128)
                    //Enables TCP keepalive for accepted connections
                    //Periodically checks if the connection is still alive
                    // Prevents Zombie connections
             .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections
            serverChannel = b.bind(port).sync();
            log.info("Network server started on port {}", port);
            
        } catch (InterruptedException e) {
            log.error("Error starting network server", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shutdown the network server
     */
    public void shutdown() {
        log.info("Shutting down network server...");
        
        if (serverChannel != null) {
            serverChannel.channel().close().awaitUninterruptibly();
        }
        
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        
        log.info("Network server stopped");
    }
}
