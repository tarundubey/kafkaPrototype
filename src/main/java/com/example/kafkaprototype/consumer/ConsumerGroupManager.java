package com.example.kafkaprototype.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages all consumer groups in the broker
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupManager implements AutoCloseable {
    private final Map<String, ConsumerGroup> consumerGroups = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    /**
     * Get or create a consumer group
     */
    public ConsumerGroup getOrCreateGroup(String groupId, OffsetManager offsetManager) {
        if (!running.get()) {
            throw new IllegalStateException("ConsumerGroupManager is shutting down");
        }
        return consumerGroups.computeIfAbsent(groupId, id -> new ConsumerGroup(id, offsetManager));
    }

    
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutting down ConsumerGroupManager");
            consumerGroups.values().forEach(ConsumerGroup::close);
            consumerGroups.clear();
            log.info("ConsumerGroupManager shutdown complete");
        }
    }
}
