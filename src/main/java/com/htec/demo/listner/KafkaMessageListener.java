package com.htec.demo.listner;

import org.springframework.kafka.annotation.KafkaListener;

public interface KafkaMessageListener {

    @KafkaListener(
            topics = {"#{__listener.topicName}"},
            groupId = "#{__listener.listenerGroupId}",
            concurrency = "#{__listener.concurrency}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    default void onKafkaMessage(String message) {
        this.onMessage(message);
    }

    void onMessage(String message);

    String getTopicName();

    String getListenerGroupId();

    String getConcurrency();
}
