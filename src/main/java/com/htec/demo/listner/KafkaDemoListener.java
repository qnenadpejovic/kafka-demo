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
    public void onMessage(@Payload String message) throws InterruptedException {
        System.out.println(message);
        if(message.equals("msg2")) {
            Thread.sleep(5000);
        } else {
            Thread.sleep(1000);
        }
        messages.add(message);
    }

    public List<String> getMessages() {
        return messages;
    }
}
