package com.example.kafkaprototype;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPrototypeApplication {
    private static final Logger log = LoggerFactory.getLogger(KafkaPrototypeApplication.class);

    public static void main(String[] args) {
        log.info("Starting Kafka Prototype Application");
        KafkaPrototypeDemo demo = new KafkaPrototypeDemo();
        try {
            demo.runDemo();
        } catch (Exception e) {
            log.error("Error running Kafka Prototype Demo", e);
        }
    }
}
