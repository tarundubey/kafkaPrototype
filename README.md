# Kafka Prototype

A simplified implementation of Apache Kafka's core concepts, including:

- Topic management
- Partitioning
- Replication
- Producer/Consumer API
- Consumer groups
- Offset management
- Network communication (using Netty)

## Features

- **Broker**: Handles topic creation, message storage, and client requests
- **Producer**: Send messages to topics with partitioning support
- **Consumer**: Subscribe to topics and consume messages in a consumer group
- **Replication**: Fault tolerance through partition replication
- **Consumer Groups**: Parallel message processing across multiple consumers
- **Offset Management**: Track consumer progress and enable message replay

## Prerequisites

- Java 21+
- Gradle 7.6+

## Notes

-  Have used Lombok annotations for keeping things short and reduce code for getters/setters and some comparisons but i am not a big fan of behind the scenes generated code

## Building the Project

```bash
# Build the project
gradle build

# Run tests
gradle test

# Run the demo application
gradle run

# Run the demo app ./gradlew :run --args='com.example.kafkaprototype.KafkaPrototypeDemo'

```

## Running the Demo

The demo showcases the following features:

1. Creates a Kafka broker on port 9092
2. Creates a topic with 3 partitions and replication factor 2
3. Starts a producer that sends messages to the topic
4. Creates two consumer groups with different numbers of consumers
5. Demonstrates message distribution across partitions and consumer groups

To run the demo:

```bash
gradle run
```

## Running Tests

Run all tests:

```bash
gradle test
```

Run a specific test class:

```bash
gradle test --tests "com.kafka.prototype.KafkaPrototypeTest"
```

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── com/kafka/prototype/
│   │       ├── broker/        # Broker implementation
│   │       ├── consumer/      # Consumer group and offset management
│   │       ├── model/         # Core data models
│   │       ├── network/       # Network communication
│   │       ├── KafkaPrototypeDemo.java  # Demo application
│   │       └── Server.java    # Main server entry point
│   └── resources/             # Configuration files
└── test/
    └── java/
        └── com/kafka/prototype/
            └── KafkaPrototypeTest.java  # Test cases
```

## Implementation Details

### Key Components

1. **Broker**: Manages topics, partitions, and handles client requests
2. **Topic**: Logical category for messages, divided into partitions
3. **Partition**: Ordered, immutable sequence of messages with replication
4. **Producer**: Publishes messages to topics with optional key-based partitioning
5. **Consumer**: Subscribes to topics and processes messages in a consumer group
6. **Consumer Group**: Coordinates message distribution among multiple consumers

### Replication

- Each partition has multiple replicas across different brokers
- One replica acts as the leader, others as followers
- Followers replicate data from the leader
- In-sync replicas (ISR) participate in the replication process

### Consumer Groups

- Multiple consumers can form a group to share the message processing load
- Each partition is consumed by only one consumer in the group
- Rebalancing occurs when consumers join or leave the group

## Limitations

This is a simplified implementation and lacks many features of the full Kafka, including:

- No disk-based storage (all messages are kept in memory)
- No authentication or authorization
- No transaction support
- No compression
- No exactly-once delivery semantics
- No cluster coordination (single broker only)
