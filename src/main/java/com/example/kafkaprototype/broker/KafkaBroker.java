package com.example.kafkaprototype.broker;

import com.example.kafkaprototype.model.Partition;
import com.example.kafkaprototype.model.Record;
import com.example.kafkaprototype.model.Topic;
import com.example.kafkaprototype.network.NetworkServer;
import com.example.kafkaprototype.consumer.ConsumerGroupManager;
import com.example.kafkaprototype.consumer.OffsetManager;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * Main broker class that manages topics and handles produce/consume requests.
 * Also delegates consumer group management to ConsumerGroupManager
 * Keeping most data members final as thread safety is must given concurrent nature of access patterns
 */
@Slf4j
public class KafkaBroker {
    // key topic name, value topic object with its partition and config info
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final int port;
    private final int brokerId;
    private final ConsumerGroupManager consumerGroupManager;
    private final OffsetManager offsetManager;
    private final AtomicBoolean isRunning = new AtomicBoolean(false); // handle race conditions because
    // it is a long-running service with multiple race condition opportunities
    private NetworkServer networkServer;

    public KafkaBroker(int port) {
        this.port = port;
        this.brokerId = (int) (System.currentTimeMillis() % Integer.MAX_VALUE);
        this.offsetManager = new OffsetManager(this);
        this.consumerGroupManager = new ConsumerGroupManager(this.offsetManager);
    }

    /**
     * Start the Kafka broker
     */
    public void start() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Broker is already running");
            return;
        }
        
        log.info("Starting Kafka broker {} on port {}", brokerId, port);

        // Initialize network server
        this.networkServer = new NetworkServer(this, port);
        this.networkServer.start();
        
        log.info("Kafka broker started successfully");
    }

    /**
     * Stop the Kafka broker
     */
    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        log.info("Stopping Kafka broker {}...", brokerId);
        
        try {
            // Stop network server
            if (networkServer != null) {
                networkServer.shutdown();
            }
            
            // Close all consumer groups
            consumerGroupManager.close();
            
            // Close all topics and partitions
            // important to prevent mem leaks
            topics.values().forEach(topic -> 
                topic.getPartitions().forEach(Partition::close)
            );
            
            log.info("Kafka broker {} stopped", brokerId);
        } catch (Exception e) {
            log.error("Error during broker shutdown", e);
        }
    }

    /**
     * Check if a topic exists
     */
    public boolean topicExists(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * Create a new topic
     */
    public void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (topics.containsKey(topic)) {
            throw new IllegalArgumentException("Topic already exists: " + topic);
        }
        
        log.info("Creating topic {} with {} partitions and replication factor {}", 
                topic, numPartitions, replicationFactor);
                
        topics.put(topic, new Topic(topic, numPartitions));
    }

    /**
     * Get a topic by name
     */
    public Topic getTopic(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        return topic;
    }

    /**
     * Produce a record to a topic
     * TODO: just for the sake of prototype  we have kept this here
     * Ideally we should create producer class to do stuff
     */
    public long produce(String topicName, Record record) {
        Topic topic = getTopic(topicName);
        
        // Simple round-robin partition selection
        // In a real implementation, this would use the record key for partitioning
        int partitionId = (int) (record.getOffset() % topic.getPartitionCount());
        Partition partition = topic.getPartition(partitionId);
        long offset = partition.append(record);
        
        log.debug("Produced record to topic={}, partition={}, offset={}", 
                 topicName, partitionId, offset);
        
        return offset;
    }

    /**
     * Consume records from a topic partition
     * TODO: just for the sake of prototype  we have kept this here
     * Ideally we should create consumer class to do stuff
     */
    public List<Record> consume(String topic, int partition, int maxRecords) {
        Topic t = topics.get(topic);
        if (t == null) {
            throw new IllegalArgumentException("Topic not found: " + topic);
        }
        
        Partition p = t.getPartition(partition);
        return p.getRecords(0, maxRecords);
    }
    
    /**
     * Consume records from a topic partition with consumer group
     * TODO: just for the sake of prototype  we have kept this here
     * Ideally we should create consumer class with group handling to do stuff
     */
    public List<Record> consume(String topic, int partition, String groupId, String consumerId, int maxRecords) {
        Topic t = topics.get(topic);
        if (t == null) {
            throw new IllegalArgumentException("Topic not found: " + topic);
        }
        
        Partition p = t.getPartition(partition);
        long offset = offsetManager.getConsumerOffset(groupId, topic, partition);
        
        List<Record> records = p.getRecords(offset, maxRecords);
        
        // Update offset
        if (!records.isEmpty()) {
            long lastOffset = records.get(records.size() - 1).getOffset();
            offsetManager.updateConsumerOffset(consumerId, topic, partition, lastOffset + 1);
        }
        
        return records;
    }
    
    public OffsetManager getOffsetManager() {
        return offsetManager;
    }
    
    public ConsumerGroupManager getConsumerGroupManager() {
        return consumerGroupManager;
    }
}
