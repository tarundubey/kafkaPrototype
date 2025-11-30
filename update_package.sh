#!/bin/bash

# Create new directory structure
echo "Creating new directory structure..."
mkdir -p src/main/java/com/example/kafkaprototype/{broker,client,consumer,model,network}
mkdir -p src/test/java/com/example/kafkaprototype

# Move files to new location
echo "Moving files to new location..."
mv src/main/java/com/kafka/prototype/*.java src/main/java/com/example/kafkaprototype/ 2>/dev/null || true
mv src/main/java/com/kafka/prototype/broker/*.java src/main/java/com/example/kafkaprototype/broker/ 2>/dev/null || true
mv src/main/java/com/kafka/prototype/client/*.java src/main/java/com/example/kafkaprototype/client/ 2>/dev/null || true
mv src/main/java/com/kafka/prototype/consumer/*.java src/main/java/com/example/kafkaprototype/consumer/ 2>/dev/null || true
mv src/main/java/com/kafka/prototype/model/*.java src/main/java/com/example/kafkaprototype/model/ 2>/dev/null || true
mv src/main/java/com/kafka/prototype/network/*.java src/main/java/com/example/kafkaprototype/network/ 2>/dev/null || true
mv src/test/java/com/kafka/prototype/*.java src/test/java/com/example/kafkaprototype/ 2>/dev/null || true

# Update package declarations and imports in all Java files
echo "Updating package declarations and imports..."
find src -name "*.java" -type f -exec sed -i '' 's/package com\.kafka\.prototype/package com.example.kafkaprototype/g' {} \;
find src -name "*.java" -type f -exec sed -i '' 's/import com\.kafka\.prototype/import com.example.kafkaprototype/g' {} \;

# Remove old directories
echo "Cleaning up old directories..."
rm -rf src/main/java/com/kafka 2>/dev/null || true
rm -rf src/test/java/com/kafka 2>/dev/null || true

echo "Package structure updated successfully!"
