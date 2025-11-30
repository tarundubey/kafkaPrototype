package com.example.kafkaprototype;

import com.example.kafkaprototype.broker.KafkaBroker;
import lombok.extern.slf4j.Slf4j;

/**
 * Main entry point for the Kafka prototype server
 * The main method is where execution begins
 * Sets up and starts the Kafka broker
 * # Start with default port (9092): java -jar kafka-prototype.jar
 * # Start with custom port: java -jar kafka-prototype.jar 9093
 */
@Slf4j
public class Server {
    private static final int DEFAULT_PORT = 9092;

    public static void main(String[] args) {
        try {
            // Parse command line arguments
            int port = parsePort(args);
            
            // Create and start the broker
            KafkaBroker broker = new KafkaBroker(port);
            broker.start();
            
            // Add shutdown hook
            // important when server is running as a service
            // so that it can be stopped gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down...");
                broker.stop();
                log.info("Shutdown complete");
            }));
            
            log.info("Kafka prototype server started on port {}", port);
            
        } catch (Exception e) {
            log.error("Error starting Kafka prototype server", e);
            System.exit(1);
        }
    }
    
    private static int parsePort(String[] args) {
        if (args.length > 0) {
            try {
                return Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                log.warn("Invalid port number: {}. Using default port {}", args[0], DEFAULT_PORT);
            }
        }
        return DEFAULT_PORT;
    }
}
