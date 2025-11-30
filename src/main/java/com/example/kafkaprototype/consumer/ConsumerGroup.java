package com.example.kafkaprototype.consumer;

import com.example.kafkaprototype.model.Record;
import com.example.kafkaprototype.model.TopicPartition;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a consumer group with multiple consumers
 */
@Slf4j
public class ConsumerGroup implements AutoCloseable {
    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private static final long SESSION_TIMEOUT_MS = 10000;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String groupId;
    private final OffsetManager offsetManager;
    private final Map<String, Set<String>> topicConsumers = new ConcurrentHashMap<>();
    private final Map<TopicPartition, String> partitionOwners = new ConcurrentHashMap<>();
    private final Map<String, Long> consumerHeartbeats = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> pendingOffsets = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();
    private final ReentrantLock rebalanceLock = new ReentrantLock();
    
    public ConsumerGroup(String groupId, OffsetManager offsetManager) {
        this.groupId = groupId;
        this.offsetManager = offsetManager;
        startHeartbeatScheduler();
    }
    
    private void startHeartbeatScheduler() {
        scheduler.scheduleAtFixedRate(this::checkConsumerLiveness,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    private void checkConsumerLiveness() {
        if (!running.get()) return;
        
        rebalanceLock.lock();
        try {
            long now = System.currentTimeMillis();
            List<String> deadConsumers = new ArrayList<>();
            
            // Find dead consumers
            for (Map.Entry<String, Long> entry : consumerHeartbeats.entrySet()) {
                if (now - entry.getValue() > SESSION_TIMEOUT_MS) {
                    deadConsumers.add(entry.getKey());
                }
            }
            
            // Process dead consumers
            if (!deadConsumers.isEmpty()) {
                log.warn("Detected dead consumers: {}", deadConsumers);
                for (String deadConsumer : deadConsumers) {
                    unregisterConsumer(deadConsumer);
                }
                rebalance();
            }
        } finally {
            rebalanceLock.unlock();
        }
    }
    
    public void heartbeat(String consumerId) {
        if (running.get() && consumerId != null) {
            rebalanceLock.lock();
            try {
                consumerHeartbeats.put(consumerId, System.currentTimeMillis());
            } finally {
                rebalanceLock.unlock();
            }
        }
    }
    
    /**
     * Register a consumer for a set of topics
     */
    /**
     * Register a consumer for a set of topics
     * @param consumerId The unique identifier for the consumer
     * @param topics The collection of topics to subscribe to
     * @throws IllegalArgumentException if consumerId or topics is null/empty
     */
    public void registerConsumer(String consumerId, Collection<String> topics) {
        if (consumerId == null || consumerId.trim().isEmpty()) {
            throw new IllegalArgumentException("Consumer ID cannot be null or empty");
        }
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Topics collection cannot be null or empty");
        }
        
        rebalanceLock.lock();
        try {
            // Add consumer to each topic's consumer set
            for (String topic : topics) {
                if (topic != null && !topic.trim().isEmpty()) {
                    topicConsumers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(consumerId);
                }
            }
            
            // Register heartbeat
            heartbeat(consumerId);
            
            // Trigger rebalance
            rebalance();
        } finally {
            rebalanceLock.unlock();
        }
    }
    
    /**
     * Unregister a consumer
     */
    /**
     * Unregister a consumer from the group
     * @param consumerId The ID of the consumer to unregister
     */
    public void unregisterConsumer(String consumerId) {
        if (consumerId == null) {
            return;
        }
        
        rebalanceLock.lock();
        try {
            // Remove from topic consumers
            for (Set<String> consumers : topicConsumers.values()) {
                consumers.remove(consumerId);
            }
            
            // Remove partition ownership
            partitionOwners.entrySet().removeIf(entry -> consumerId.equals(entry.getValue()));
            
            // Remove heartbeat
            consumerHeartbeats.remove(consumerId);
            
            // Trigger rebalance if group is still active
            if (running.get()) {
                rebalance();
            }
        } finally {
            rebalanceLock.unlock();
        }
    }
    
    /**
     * Rebalance partitions among consumers
     */
    /**
     * Rebalance partitions among consumers in the group.
     * This is called when:
     * 1. A new consumer joins the group
     * 2. An existing consumer leaves the group
     * 3. New partitions are added to subscribed topics
     */
    private void rebalance() {
        if (!running.get()) {
            return;
        }
        
        rebalanceLock.lock();
        try {
            // In a real implementation, this would be more sophisticated
            // Here we'll do a simple round-robin assignment
            
            // Clear existing assignments
            partitionOwners.clear();
            
            for (Map.Entry<String, Set<String>> entry : topicConsumers.entrySet()) {
                String topic = entry.getKey();
                Set<String> consumers = entry.getValue();
                
                if (consumers == null || consumers.isEmpty()) {
                    continue;
                }
                
                // Get all partitions for this topic (from pending or committed offsets)
                Set<Integer> partitions = new HashSet<>();
                
                // Find all partitions for this topic from pending offsets
                for (TopicPartition tp : pendingOffsets.keySet()) {
                    if (tp != null && topic.equals(tp.getTopic())) {
                        partitions.add(tp.getPartition());
                    }
                }
                    
                // Find all partitions for this topic from committed offsets
                for (TopicPartition tp : committedOffsets.keySet()) {
                    if (tp != null && topic.equals(tp.getTopic())) {
                        partitions.add(tp.getPartition());
                    }
                }
                
                if (partitions.isEmpty()) {
                    continue;
                }
                
                // Assign partitions to consumers in round-robin fashion
                int consumerIndex = 0;
                List<String> consumerList = new ArrayList<>(consumers);
                
                for (int partition : partitions) {
                    if (consumerList.isEmpty()) {
                        break;
                    }
                    
                    String assignedConsumer = consumerList.get(consumerIndex % consumerList.size());
                    TopicPartition tp = new TopicPartition(topic, partition);
                    
                    // Update ownership
                    partitionOwners.put(tp, assignedConsumer);
                    
                    log.debug("Assigned partition {} of topic {} to consumer {}", 
                            partition, topic, assignedConsumer);
                    
                    consumerIndex++;
                }
            }
        } finally {
            rebalanceLock.unlock();
        }
    }
    
    /**
     * Get the partitions assigned to a specific consumer
     */
    public synchronized Map<String, List<Integer>> getAssignedPartitions(String consumerId) {
        Map<String, List<Integer>> assignments = new HashMap<>();
        
        for (Map.Entry<TopicPartition, String> entry : partitionOwners.entrySet()) {
            if (entry.getValue().equals(consumerId)) {
                TopicPartition tp = entry.getKey();
                assignments.computeIfAbsent(tp.getTopic(), k -> new ArrayList<>())
                          .add(tp.getPartition());
            }
        }
        
        return assignments;
    }
    
    /**
     * Update the latest seen offset for a topic partition
     */
    public void updateOffset(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        committedOffsets.put(tp, offset);
    }
    
    /**
     * Get the current offset for a topic partition
     */
    public long getOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return committedOffsets.getOrDefault(tp, 0L);
    }
    
    /**
     * Commit offsets for a consumer
     */
    public synchronized void commitOffsets(String consumerId, Map<TopicPartition, Long> offsets) {
        if (!running.get()) {
            throw new IllegalStateException("Consumer group is shutting down");
        }
        
        // Store offsets in the pending map
        pendingOffsets.putAll(offsets);
        
        // Filter offsets for partitions owned by this consumer
        Map<TopicPartition, Long> ownedOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            if (consumerId.equals(partitionOwners.get(tp))) {
                ownedOffsets.put(tp, entry.getValue());
                log.debug("Committing offset {} for {}-{}", entry.getValue(), tp.getTopic(), tp.getPartition());
            } else {
                log.warn("Consumer {} does not own partition {}-{}", consumerId, tp.getTopic(), tp.getPartition());
            }
        }
        
        // Commit to offset manager
        if (!ownedOffsets.isEmpty()) {
            offsetManager.commitOffsets(groupId, ownedOffsets);
            
            // Move from pending to committed
            for (Map.Entry<TopicPartition, Long> entry : ownedOffsets.entrySet()) {
                pendingOffsets.remove(entry.getKey());
                committedOffsets.put(entry.getKey(), entry.getValue());
            }
        }
    }
    
    /**
     * Get the last committed offset for a topic partition
     */
    public long getCommittedOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        
        // First check pending offsets (not yet committed)
        Long pendingOffset = pendingOffsets.get(tp);
        if (pendingOffset != null) {
            return pendingOffset;
        }
        
        // Then check committed offsets
        return committedOffsets.getOrDefault(tp, 
            offsetManager.getCommittedOffset(groupId, topic, partition));
    }
    
    /**
     * Shuts down the consumer group and releases all resources.
     * This method blocks until all pending operations complete or the timeout elapses.
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            try {
                // Shutdown the scheduler
                scheduler.shutdown();
                
                // Wait for tasks to complete with timeout
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    // Force shutdown if tasks don't complete in time
                    List<Runnable> pendingTasks = scheduler.shutdownNow();
                    if (!pendingTasks.isEmpty()) {
                        log.warn("Cancelled {} pending tasks during shutdown", pendingTasks.size());
                    }
                }
                
                // Clear all state
                rebalanceLock.lock();
                try {
                    topicConsumers.clear();
                    partitionOwners.clear();
                    consumerHeartbeats.clear();
                    pendingOffsets.clear();
                    // Don't clear committedOffsets as they're persisted in OffsetManager
                } finally {
                    rebalanceLock.unlock();
                }
                
                log.info("Consumer group {} shutdown complete", groupId);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while shutting down consumer group {}", groupId, e);
                // Re-assert the interrupt status
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error during consumer group shutdown", e);
            }
        }
    }
    
    /**
     * Add a new partition to the group
     */
    public synchronized void addPartition(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        committedOffsets.putIfAbsent(tp, 0L);
        
        // Trigger rebalance when new partitions are added
        rebalance();
    }
}
