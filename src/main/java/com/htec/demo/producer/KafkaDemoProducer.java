package com.htec.demo.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaDemoProducer {

    @Autowired
    private KafkaTemplate<String, String> producer;

    public void sendMessage(String message) {
        producer.send(new ProducerRecord<>("demo-topic", message));
    }
}
