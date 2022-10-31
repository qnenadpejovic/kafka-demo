package com.htec.demo.listner;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.LinkedList;
import java.util.List;

public class KafkaDemoListener {
    private List<String> messages;

    public KafkaDemoListener() {
        messages = new LinkedList<>();
    }

    @KafkaListener(
            topics = {"demo-topic"},
            groupId = "demo-test-groupId",
            concurrency = "1",
            containerFactory = "customKafkaListenerContainerFactory"
    )
    public void onMessage(@Payload String message, Acknowledgment acknowledgment) throws InterruptedException {
        onMessageManualImmediateAck(message, acknowledgment);
    }

    private void onMessageManualImmediateAck(String message, Acknowledgment acknowledgment) throws InterruptedException {
        System.out.println(message);
        acknowledgment.acknowledge();
        messages.add(message);
    }

    public List<String> getMessages() {
        return messages;
    }

}
