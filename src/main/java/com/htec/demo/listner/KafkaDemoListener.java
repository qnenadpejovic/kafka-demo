package com.htec.demo.listner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class KafkaDemoListener implements KafkaMessageListener{

    private String topicName;
    private String listenerGroupId;
    private String concurrency;
    private List<String> messages;

    public KafkaDemoListener(String topicName, String listenerGroupId, String concurrency) {
        this.topicName = topicName;
        this.listenerGroupId = listenerGroupId;
        this.concurrency = concurrency;
        messages = new LinkedList<>();
    }

    public void onMessage(String message) {
        System.out.println("message received: " + message);
        messages.add(message);
    }

    public String getTopicName() {
        return topicName;
    }

    public String getListenerGroupId() {
        return listenerGroupId;
    }

    public String getConcurrency() {
        return concurrency;
    }

    public List<String> getMessages() {
        return messages;
    }

}
