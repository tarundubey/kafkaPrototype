Checkout my [medium article for this prototype here ](https://medium.com/@tarunkumardubey6793/understanding-kafka-from-scratch-concepts-flow-and-real-examples-818fea6f8504)

You can [check the dev handbook in this doc](https://docs.google.com/document/d/11wsnakDdhI847snB4WvImbg5tpBRpH6WCd0-0q04vEk/edit?tab=t.0)

# Kafka Prototype

A simplified implementation of Apache Kafka's core concepts, including:

## Features

- **Broker**: Handles topic creation, message storage, and client requests
- **Producer**: Send messages to topics with partitioning support
- **Consumer**: Subscribe to topics and consume messages in a consumer group
- **Network Server**: Handles incoming client connections using Netty
- **Logging**: Comprehensive logging using SLF4j with Logback
- **Concurrency**: Thread-safe operations using Java's concurrent utilities

## Prerequisites

- Java 17+
- Gradle 7.6+
- Lombok plugin for your IDE (for annotation processing)

## Notes

- Uses Lombok annotations to reduce boilerplate code
- Implements a simplified version of Kafka's network protocol
- Designed for educational purposes to understand Kafka's internals

## Building the Project

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run the demo application
./gradlew bootRun

# Or run the demo directly
./gradlew :run --args='com.example.kafkaprototype.KafkaPrototypeDemo'
```

## Running the Demo

The demo showcases the following features:

1. Creates a Kafka broker with a network server
2. Demonstrates basic producer/consumer interaction
3. Shows message handling through the Netty pipeline
4. Includes example of request/response flow

To run the demo:

```bash
./gradlew bootRun

# Or run the demo class directly
./gradlew :run --args='com.example.kafkaprototype.KafkaPrototypeDemo'
```

## Running Tests

Run all tests:

```bash
./gradlew test
```

Run a specific test class:

```bash
./gradlew test --tests "com.example.kafkaprototype.*Test"
```

View test coverage report:

```bash
./gradlew test jacocoTestReport
# Open build/reports/jacoco/test/html/index.html in your browser
```

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── com/example/kafkaprototype/
│   │       ├── broker/                # Broker implementation
│   │       ├── consumer/              # Consumer and consumer group management
│   │       ├── network/               # Netty-based network server and handlers
│   │       │   ├── codec/             # Request/response encoding/decoding
│   │       │   └── handler/           # Channel handlers for processing requests
│   │       ├── KafkaPrototypeDemo.java  # Demo application
│   │       ├── Server.java            # Main server entry point
│   │       └── KafkaPrototypeApplication.java  # Spring Boot entry point (if applicable)
│   └── resources/                     # Configuration files
└── test/
    └── java/
        └── com/example/kafkaprototype/
            └── *Test.java             # Test cases
```

## Implementation Details

### Key Components

1. **Network Server**: Handles incoming client connections using Netty's NIO
   - Uses `NioEventLoopGroup` for handling I/O operations
   - Implements a custom pipeline with request decoders and response encoders
   - Manages channel lifecycle and error handling

2. **Request Handling**:
   - `KafkaRequestDecoder`: Decodes incoming byte streams into request objects
   - `KafkaResponseEncoder`: Encodes response objects into byte streams
   - `KafkaRequestHandler`: Processes requests and generates responses

3. **Broker Core**:
   - Manages topics and partitions
   - Handles producer and consumer coordination
   - Implements basic message storage and retrieval

4. **Consumer Groups**:
   - Basic consumer group management
   - Offset tracking for consumer positions
   - Partition assignment to consumers

## Limitations

This is a simplified implementation for educational purposes and lacks many production-grade features:

- In-memory storage only (no disk persistence)
- Single broker (no clustering or replication)
- Basic error handling and recovery
- No authentication or authorization
- No transaction support
- No message compression
- No exactly-once delivery semantics
- Limited protocol support (only basic request/response types)
- No support for Kafka's binary protocol in its entirety

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
