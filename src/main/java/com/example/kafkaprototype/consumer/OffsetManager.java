package com.example.kafkaprototype.consumer;

import com.example.kafkaprototype.broker.KafkaBroker;
import com.example.kafkaprototype.model.TopicPartition;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages durable storage of consumer offsets
 */
@Slf4j
@Data
public class OffsetManager {
    private static final String OFFSETS_TOPIC = "__consumer_offsets";
    private final KafkaBroker broker;
    private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Long>> consumerGroupOffsets = new ConcurrentHashMap<>();
    
    public OffsetManager(KafkaBroker broker) {
        this.broker = broker;
        initializeOffsetsTopic();
    }
    
    /**
     * Commit offset for a consumer group
     */
    public void commitOffset(String groupId, String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        committedOffsets.put(tp, offset);
        
        // Also update consumer group offset
        consumerGroupOffsets
            .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
            .put(partition, offset);
            
        log.debug("Committed offset {} for group {} on {}-{}", offset, groupId, topic, partition);
    }
    
    /**
     * Get the last committed offset for a topic-partition
     */
    public long getCommittedOffset(String groupId, String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return committedOffsets.getOrDefault(tp, 0L);
    }

    
    /**
     * Update the offset for a consumer group
     */
    public void updateConsumerOffset(String groupId, String topic, int partition, long offset) {
        consumerGroupOffsets
            .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
            .put(partition, offset);
    }
    
    /**
     * Get the current offset for a consumer group
     */
    public long getConsumerOffset(String groupId, String topic, int partition) {
        return consumerGroupOffsets
            .getOrDefault(groupId, Map.of())
            .getOrDefault(partition, 0L);
    }
    
    /**
     * Commit multiple offsets for a consumer group
     */
    public void commitOffsets(String groupId, Map<TopicPartition, Long> offsetsToCommit) {
        for (Map.Entry<TopicPartition, Long> entry : offsetsToCommit.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue();
            commitOffset(groupId, tp.getTopic(), tp.getPartition(), offset);
        }
    }
    
    private void initializeOffsetsTopic() {
        // Check if the offsets topic exists, create if it doesn't
        if (!broker.topicExists(OFFSETS_TOPIC)) {
            broker.createTopic(OFFSETS_TOPIC, 50, (short) 1);
            log.info("Created offsets topic: {}", OFFSETS_TOPIC);
        }
    }
}
