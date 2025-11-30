package com.example.kafkaprototype.model;

import lombok.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The most important model/class that is used across every component in Kafka
 * Represents a single record/message or event in our Kafka prototype.
 * Look and compare with the document to establish analogy
 * kept things comprehensive to include all components of messages
 * Be mindful of the fact that Record/Event has to be immutable in every sense after it is created
 * You will see things like creating defensive copy for headers and deep copy for values in headers .
 * TODO Evaluate if fields should be final and we enforce immutability
 */
@Data
@NoArgsConstructor
public class Record {
    @Data
    @AllArgsConstructor
    /*
    Headers provide additional metadata such as trace IDs, messageType/version
    that may be needed for conditional processing
    The reason why it is kept as separate class is primarily to separate out business data which is present in
    key,value versus system/operational data which is present in headers.
    Headers are NOT defined in the Avro schema.
    They are a separate, distinct component of the Kafka message and hence consumer can use info in headers to even
    decide if they want to open/deser the message or not. Hence it has it's own simple equals, hashCode or other deser
    methods.
    We have intentionally kept this class static to create them independently and Record doesn't
    necessarily depend on it and we avoid memory leaks
     */
    public static class Header {
        @Getter
        private String key;
        private byte[] value;


        public byte[] getValue() {
            return value != null ? value.clone() : null;
        }

        // We are overriding these to use for comparing/deduplicating in Consumer/Producer
        @Override
        public boolean equals(Object o) {
            if (this == o) return true; // same obj as caller
            if (!(o instanceof Header)) return false;
            Header header = (Header) o;
            return Objects.equals(key, header.key) && 
                   Objects.deepEquals(value, header.value);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(key, value != null ? value.length : 0);
        }
    }
    
    private long offset;
    private long timestamp;
    private String key;
    //We are taking value as byte array as it is the best way to represent any format of data
    // such as JSON/Avro/Protobuf etc. It is optimum for network transfer and storage.
    private byte[] value;
    private Header[] headers;


    public Record(long offset, long timestamp, String key, byte[] value, Header[] headers) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value != null ? value.clone() : null;
        // do a defensive deep copy - records pass through multiple threads/systems and need to be
        // immutable in all sense
        if (headers != null) {
            this.headers = new Header[headers.length];
            for (int i = 0; i < headers.length; i++) {
                Header h = headers[i];
                this.headers[i] = new Header(h.getKey(), h.getValue() != null ? h.getValue().clone() : null);
            }
        } else {
            this.headers = null;
        }
    }

    // We will use these methods for simulating partitions logic such as reading/storing

    public int size() {
        // 2 Long for ts and offset
        return Long.BYTES * 2 
            + (key != null ? key.getBytes(StandardCharsets.UTF_8).length : 0) 
            + (value != null ? value.length : 0)
            + calculateHeadersSize();
    }

    private int calculateHeadersSize() {
        if (headers == null) return 0;
        int size = 0;
        for (Header header : headers) {
            size += (header.key != null ? header.key.getBytes(StandardCharsets.UTF_8).length : 0)
                  + (header.value != null ? header.value.length : 0);
        }
        return size;
    }

//    We have used byteBuffers for readability and avoiding direct byte manipulation
//    We get a lot of helpers for data types in byte buffer to make ser/deser simpler
//    fromBytes is reading byte stored coming in as byteBuffer and returns corresponding Record/Event
    public static Record fromBytes(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        
        // Deserialize from bytes
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();
        
        // Read key
        int keyLength = buffer.getInt();
        String key = null;
        if (keyLength > 0) {
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }
        
        // Read value
        int valueLength = buffer.getInt();
        byte[] value = null;
        if (valueLength > 0) {
            value = new byte[valueLength];
            buffer.get(value);
        }
        
        // Read headers
        int numHeaders = buffer.getInt();
        Header[] headers = numHeaders > 0 ? new Header[numHeaders] : null;
        
        if (numHeaders > 0) {
            for (int i = 0; i < numHeaders; i++) {
                int headerKeyLength = buffer.getInt();
                byte[] headerKeyBytes = new byte[headerKeyLength];
                buffer.get(headerKeyBytes);
                String headerKey = new String(headerKeyBytes, StandardCharsets.UTF_8);
                
                int headerValueLength = buffer.getInt();
                byte[] headerValue = headerValueLength > 0 ? new byte[headerValueLength] : null;
                if (headerValueLength > 0) {
                    buffer.get(headerValue);
                }
                
                headers[i] = new Header(headerKey, headerValue);
            }
        }
        
        return new Record(offset, timestamp, key, value, headers);
    }
    
    public byte[] toBytes() {
        // Calculate total size
        int size = Long.BYTES * 2; // offset + timestamp
        
        // Add key size
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
        size += Integer.BYTES + keyBytes.length;
        
        // Add value size
        byte[] valueBytes = value != null ? value : new byte[0];
        size += Integer.BYTES + valueBytes.length;
        
        // Add headers size
        int numHeaders = headers != null ? headers.length : 0;
        size += Integer.BYTES; // num headers
        
        // Calculate header sizes
        byte[][] headerKeyBytes = new byte[numHeaders][];
        byte[][] headerValueBytes = new byte[numHeaders][];
        
        for (int i = 0; i < numHeaders; i++) {
            Header header = headers[i];
            headerKeyBytes[i] = header.getKey() != null ? 
                header.getKey().getBytes(StandardCharsets.UTF_8) : new byte[0];
            headerValueBytes[i] = header.getValue() != null ? 
                header.getValue() : new byte[0];
                
            size += Integer.BYTES * 2 + headerKeyBytes[i].length + headerValueBytes[i].length;
        }
        
        // Allocate buffer and write data
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        
        // Write key
        buffer.putInt(keyBytes.length);
        if (keyBytes.length > 0) {
            buffer.put(keyBytes);
        }
        
        // Write value
        buffer.putInt(valueBytes.length);
        if (valueBytes.length > 0) {
            buffer.put(valueBytes);
        }
        
        // Write headers
        buffer.putInt(numHeaders);
        for (int i = 0; i < numHeaders; i++) {
            buffer.putInt(headerKeyBytes[i].length);
            if (headerKeyBytes[i].length > 0) {
                buffer.put(headerKeyBytes[i]);
            }
            
            buffer.putInt(headerValueBytes[i].length);
            if (headerValueBytes[i].length > 0) {
                buffer.put(headerValueBytes[i]);
            }
        }

//        TODO: Evaluate if we want to return raw bytes like we are doing now vs returning Byte Buffer directly
        
        return buffer.array();
    }
}
