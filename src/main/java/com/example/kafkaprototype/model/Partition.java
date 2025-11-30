package com.example.kafkaprototype.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a partition of a topic in our Kafka prototype.
 * We will try to enforce immutability through final variables
 * We also have concurrency use cases (Concurrent Queue and Atomic Long)
 * for which we wont need explicit synchronization for final fields
 * and we prevent accidental modifications.
 * We are not doing segmentation based on size or time and using single log file per partition for simplicity
 * TODO: Try to implement segment rolling and index files for random access
 * TODO: Try implementing Log compaction and time based retention
 * TODO: Implement log cleaning
 * In current implementation we append records sequentially and maintain In mem queue
 * which primarily serves reads
 */
@Slf4j
@Data
public class Partition {
    private final int id;
    private final String topic;
//    Using atomic long to ensure atomic updates to HW without explicit sync
//    IMO - more efficient for our use case compared to synchronized block
//    Same goes for concurrent linked queue where multiple concurrent threads can make
//    appends and reads can happen in parallel efficiently due to its lock free nature
    private final AtomicLong highWatermark = new AtomicLong(-1);
    private final Queue<Record> inMemoryRecords = new ConcurrentLinkedQueue<>();
    private final String dataDir = "data";
//    Unlike memory mapped file which Kafka uses which of course is more performant (on  top of that it does
//    better batching / compression with  WALs)
//  We are using file channel and Random access file for simplicity and performance
    private FileChannel logFileChannel;
    private RandomAccessFile logFile;
    private long filePosition = 0;
//    While atomic long and concurrent queue are thread safe
//    we use explicit lock for "group" write operations to ensure thread safety
    private final Object writeLock = new Object();
    
    // Replication state
    private final int replicaId; // used to determine if message is from leader or follower
    private final List<Replica> replicas = new ArrayList<>(); // used for replica sync and determine HW - it is a list of all replicas for current parition
    private volatile boolean isLeader = false; // in case there are multiple threads - we use volatile for viz
    private volatile long leaderEpoch = 0; // track leadership changes for partition instance (Zombie leaders)

    // Static for obvious reasons - makes sense coupled with partition but its data members
    // are better grouped together rather than keeping it in Partition class imo
    public static class Replica {
        private final int brokerId;
        private long logEndOffset = -1;
        private long lastFetchTime = System.currentTimeMillis();
        
        public Replica(int brokerId) {
            this.brokerId = brokerId;
        }
        
        // Getters and setters
        public int getBrokerId() { return brokerId; }
        public long getLogEndOffset() { return logEndOffset; }
        public void setLogEndOffset(long offset) { this.logEndOffset = offset; }

        //TODO: we may use getLastFetchTime to determine if replica is in sync with leader or lagging later
        public long getLastFetchTime() { return lastFetchTime; }
        public void updateLastFetchTime() { this.lastFetchTime = System.currentTimeMillis(); }
    }

    public Partition(String topic, int partitionId, int replicaId, boolean isLeader) {
        this.topic = topic;
        this.id = partitionId;
        this.replicaId = replicaId;
        this.isLeader = isLeader;
        if (isLeader) {
            this.leaderEpoch++;
        }
        initializeLogFile();
    }

    private void initializeLogFile() {
        try {
            // Create data directory if it doesn't exist
            Path dataPath = Paths.get(dataDir);
            if (!Files.exists(dataPath)) {
                Files.createDirectories(dataPath);
            }

            // Create or open the log file for this partition
            //The File class is used purely for path manipulation and abstract file representation
            File logFile = new File(dataDir, String.format("partition-%d.log", id));
            //RandomAccess file has capability to opens the file with simultaneous read-write ("rw") permissions,
            // with capabilities to read and write at any position in the file and the current file size
            this.logFile = new RandomAccessFile(logFile, "rw");
            this.logFileChannel = this.logFile.getChannel();
            this.filePosition = logFile.length();
            
            log.info("Initialized partition {} log file at {}", id, logFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Error initializing log file for partition {}", id, e);
            throw new RuntimeException("Failed to initialize log file", e);
        }
    }

    /**
     * Append a record to this partition if the message is from leader - the write use case when
     * producer writes to broker
     */
    public long append(Record record) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot append to a non-leader replica");
        }
        
        synchronized (writeLock) {
            try {
                // Set the offset for this record where it is is written in log file
                long offset = highWatermark.incrementAndGet();
                record.setOffset(offset);
                
                // Serialize the record to bytes
                byte[] recordBytes = record.toBytes();
                ByteBuffer buffer = ByteBuffer.allocate(4 + recordBytes.length);
                buffer.putInt(recordBytes.length);
                buffer.put(recordBytes);
                // after writing, reset buffer pos to 0 so the flush to file happens from the beginning
                // Miss this and you wont write anything to file

                buffer.flip();
                
                // Write to the log file
                while (buffer.hasRemaining()) {
                    // The superiority of channels
                    //Executes the high-performance, non-blocking writing of data from the ByteBuffer and ensures
                    // it is physically saved to disk.
                    // is preferred over RandomAccessFile.write() methods because
                    // it interfaces directly with operating system native I/O routines,
                    // often bypassing some Java I/O overhead - Source is official doc and library
                    filePosition += logFileChannel.write(buffer, filePosition);
                }
                
                // Force write to disk
                logFileChannel.force(true);
                
                // Keep in memory for fast access
                inMemoryRecords.add(record);
                
                // Update replicas asynchronously
                updateReplicas(record, offset);
                
                log.debug("Appended record with offset {} to partition {}-{}", offset, topic, id);
                return offset;
            } catch (IOException e) {
                log.error("Error appending record to partition {}-{}", topic, id, e);
                throw new RuntimeException("Failed to append record", e);
            }
        }
    }
    
    /**
     * Update follower replicas with the new record - again only if it is from leader
     */
    private void updateReplicas(Record record, long offset) {
        if (!isLeader) {
            return;
        }
        
        // In a real implementation, this would be done asynchronously
        // and would wait for acknowledgments from followers but for this prototype
        // we are gonna just update the replica's state directly
        for (Replica replica : replicas) {
            if (replica.getBrokerId() != this.replicaId) { // Don't send to leader itself or get stuck in a loop
                try {
                    // In a real implementation, we would send the record to the follower
                    // and wait for acknowledgment before updating the high watermark
                    replica.setLogEndOffset(offset);
                    replica.updateLastFetchTime();
                    
                    log.trace("Updated replica {} for partition {}-{} to offset {}", 
                            replica.getBrokerId(), topic, id, offset);
                } catch (Exception e) {
                    log.error("Error updating replica {} for partition {}-{}", 
                            replica.getBrokerId(), topic, id, e);
                }
            }
        }
        
        // Update high watermark based on in-sync replicas
        updateHighWatermark();
    }
    
    /**
     * Update the high watermark based on in-sync replicas
     */
    private void updateHighWatermark() {
        if (!isLeader) {
            return;
        }
        
        // Find the minimum log end offset among all in-sync replicas
        long minReplicaOffset = highWatermark.get();
        for (Replica replica : replicas) {
            if (replica.getLogEndOffset() < minReplicaOffset) {
                minReplicaOffset = replica.getLogEndOffset();
            }
        }
        
        // Update high watermark if we have a quorum
        if (minReplicaOffset > highWatermark.get()) {
            highWatermark.set(minReplicaOffset);
            log.debug("Updated high watermark for partition {}-{} to {}", 
                    topic, id, minReplicaOffset);
        }
    }

    
    /**
     * Get records from this partition starting from the given offset with a maximum record count
     * @return List of records up to the maximum count
     * Note that maxRecords is more of a count representation
     */
    public List<Record> getRecords(long startOffset, int maxRecords) {
        List<Record> allRecords = new ArrayList<>();
        
        // First check in-memory records
        for (Record record : inMemoryRecords) {
            if (record.getOffset() >= startOffset) {
                allRecords.add(record);
                if (allRecords.size() >= maxRecords) {
                    break;
                }
            }
        }
        
        // If we need more records, read from disk
        if (allRecords.size() < maxRecords) {
            try (RandomAccessFile file = new RandomAccessFile(
                    new File(dataDir, String.format("partition-%d.log", id)), "r")) {
                
                // Skip to the requested offset
                file.seek(0); // Start from beginning
                //In Kafka where there is actual index file that points to multiple offsets
                // we would do pointed seek - here for simplicity we seek to 0 and progress
                while (file.getFilePointer() < file.length() && allRecords.size() < maxRecords) {
                    long recordOffset = file.readLong();
                    if (recordOffset < startOffset) {
                        // Skip this record
                        int recordSize = file.readInt();
                        file.skipBytes(recordSize);
                        continue;
                    }
                    
                    // Read record size
                    int recordSize = file.readInt();
                    
                    // Read record data
                    byte[] recordData = new byte[recordSize];
                    file.readFully(recordData);
                    
                    // Deserialize record
                    Record record = Record.fromBytes(ByteBuffer.wrap(recordData));
                    allRecords.add(record);
                }
            } catch (IOException e) {
                log.error("Error reading records from disk for partition {}", id, e);
                throw new RuntimeException("Failed to read records from disk", e);
            }
        }
        return allRecords;
        
    }

    /**
     * Close resources
     */
    public void close() {
        try {
            if (logFileChannel != null) {
                logFileChannel.close();
            }
            if (logFile != null) {
                logFile.close();
            }
        } catch (IOException e) {
            log.error("Error closing partition {}", id, e);
        }
    }
}
