package com.example.kafkaprototype;

import com.example.kafkaprototype.broker.KafkaBroker;
import com.example.kafkaprototype.consumer.ConsumerGroup;
import com.example.kafkaprototype.model.Record;
import lombok.extern.slf4j.Slf4j;

import com.example.kafkaprototype.model.TopicPartition;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A demonstration class showcasing the features of the Kafka prototype.
 * This demo creates a Kafka broker, produces messages to topics, and consumes them
 * using multiple consumer groups and partitions.
 */
@Slf4j
public class KafkaPrototypeDemo {
    private static final int BROKER_PORT = 9092;
    private static final String DEMO_TOPIC = "demo-topic";
    private static final String GROUP_1 = "demo-group-1";
    private static final String GROUP_2 = "demo-group-2";
    private static final int NUM_PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 2;
    private static final int MESSAGES_PER_PRODUCER = 10;
    
    private final KafkaBroker broker;
    private final ExecutorService executor;
    private final CountDownLatch completionLatch;
    private final AtomicInteger messageCounter = new AtomicInteger(0);

    public static void main(String[] args) {
        try {
            new KafkaPrototypeDemo().runDemo();
        } catch (Exception e) {
            log.error("Demo failed", e);
        }
    }

    public KafkaPrototypeDemo() {
        this.broker = new KafkaBroker(BROKER_PORT);
        this.executor = Executors.newCachedThreadPool();
        this.completionLatch = new CountDownLatch(1);
    }

    public void runDemo() throws Exception {
        log.info("=== Starting Kafka Prototype Demo ===");
        
        try {
            // Start the broker
            broker.start();
            log.info("Started Kafka broker on port {}", BROKER_PORT);

            // Create a topic
            broker.createTopic(DEMO_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
            log.info("Created topic '{}' with {} partitions and replication factor {}", 
                    DEMO_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);

            // Start producer
            startProducer("producer-1");
            
            // Start consumer groups
            startConsumerGroup(GROUP_1, 2);  // Group 1 with 2 consumers
            startConsumerGroup(GROUP_2, 1);  // Group 2 with 1 consumer

            // Wait for demo to complete
            completionLatch.await(30, TimeUnit.SECONDS);
            
        } finally {
            shutdown();
        }
    }

    private void startProducer(String producerId) {
        executor.submit(() -> {
            try {
                for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
                    String key = "key-" + i;
                    String value = String.format("Message %d from %s at %s", 
                            i, producerId, new Date());
                    
                    Record record = new Record(
                            0, // timestamp type
                            System.currentTimeMillis(),
                            key,
                            value.getBytes(StandardCharsets.UTF_8),
                            null // headers
                    );
                    
                    broker.produce(DEMO_TOPIC, record);
                    log.info("Produced: {} -> {}", key, value);
                    
                    messageCounter.incrementAndGet();
                    Thread.sleep(500); // Slow down for demo purposes
                }
                
                // Signal that production is complete
                completionLatch.countDown();
                
            } catch (Exception e) {
                log.error("Producer error", e);
            }
        });
    }

    private void startConsumerGroup(String groupId, int numConsumers) {
        for (int i = 0; i < numConsumers; i++) {
            String consumerId = groupId + "-consumer-" + i;
            executor.submit(() -> {
                ConsumerGroup group = broker.getConsumerGroupManager().getOrCreateGroup(groupId, broker.getOffsetManager());
                group.registerConsumer(consumerId, Collections.singletonList(DEMO_TOPIC));
                
                log.info("Consumer {} joined group {}", consumerId, groupId);
                
                try {
                    while (completionLatch.getCount() > 0) {
                        // Get assigned partitions
                        var assignments = group.getAssignedPartitions(consumerId);
                        
                        // Process each assigned partition
                        for (var entry : assignments.entrySet()) {
                            String topic = entry.getKey();
                            for (int partition : entry.getValue()) {
                                // Fetch records from the partition
                                var records = broker.consume(topic, partition, 5);
                                
                                // Process records
                                if (!records.isEmpty()) {
                                    for (Record record : records) {
                                        String key = record.getKey() != null ? 
                                                record.getKey() : "null";
                                        String value = record.getValue() != null ? 
                                                new String(record.getValue(), StandardCharsets.UTF_8) : "null";
                                                
                                        log.info("[{}][{}] {}: {}", 
                                                groupId, 
                                                consumerId, 
                                                key, 
                                                value);
                                                
                                        // Simulate processing time
                                        Thread.sleep(100);
                                    }
                                    
                                    // Commit offsets for the last record
                                    Record lastRecord = records.get(records.size() - 1);
                                    Map<TopicPartition, Long> offsets = new HashMap<>();
                                    offsets.put(new TopicPartition(topic, partition), lastRecord.getOffset() + 1);
                                    group.commitOffsets(consumerId, offsets);
                                }
                            }
                        }
                        
                        // Send heartbeat
                        group.heartbeat(consumerId);
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    log.error("Consumer error", e);
                }
            });
        }
    }

    private void shutdown() {
        log.info("Shutting down demo...");
        try {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate gracefully");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            broker.stop();
            log.info("Demo completed. Total messages produced: {}", messageCounter.get());
        }
    }
}
