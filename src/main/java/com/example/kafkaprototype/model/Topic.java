package com.example.kafkaprototype.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a topic in our Kafka prototype.
 * Final variables with atomic counter for uniq message ids
 * WHy? Multiple producer/consumer threads can access Topics and it's partitions through brokers
 */
@Data
public class Topic {
    private final String name;
    private final int numPartitions;
    private final List<Partition> partitions;
    private final AtomicLong messageCounter = new AtomicLong(0); //Counter for generating unique message IDs

    public Topic(String name, int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.partitions = new ArrayList<>(numPartitions);
        
        // Initialize partitions
        for (int i = 0; i < numPartitions; i++) {
            // Create a new partition with default replica ID of 0 (will be updated by the broker)
            // The broker will set the correct leader/replica information
            // Parameters: topic, partitionId, replicaId, isLeader
            partitions.add(new Partition(name, i, 0, true));
        }
    }

    /**
     * Get a partition by its ID
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 0 || partitionId >= numPartitions) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
        return partitions.get(partitionId);
    }

    /**
     * Get the partition count for this topic
     */
    public int getPartitionCount() {
        return numPartitions;
    }
}
