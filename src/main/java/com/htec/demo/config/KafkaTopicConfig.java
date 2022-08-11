package com.htec.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic reportTopic() {

        String topicName = "demo-topic";
        int partitions = 10;
        short replicationFactor = 1;

        return new NewTopic(topicName, partitions, replicationFactor);
    }
}
