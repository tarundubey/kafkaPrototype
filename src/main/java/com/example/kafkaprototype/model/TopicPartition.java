package com.example.kafkaprototype.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Represents a topic-partition pair used for offset tracking, ensuring unique identification
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class TopicPartition {
    private final String topic;
    private final int partition;

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
