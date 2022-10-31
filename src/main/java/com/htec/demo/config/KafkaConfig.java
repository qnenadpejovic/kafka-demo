package com.htec.demo.config;

import com.htec.demo.listner.KafkaDemoListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String boostrapServers;

    @Value("${spring.kafka.auto.offset.reset:latest}")
    private String autoOffsetSetting;

    @Bean(name="producerFactory")
    public ProducerFactory<String, String> getProducerFactory() {

        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put("max.block.ms", 15000);
        producerProperties.put("request.timeout.ms", 15000);
        producerProperties.put("bootstrap.servers", getBootstrapServers());
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 5);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("enable.idempotency", true);

        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(getProducerFactory());
    }

    private String getBootstrapServers() {
        return boostrapServers;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> customKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory((ConsumerFactory<? super String, ? super String>) autoCommitConsumerFactory());
        factory.setCommonErrorHandler(new DefaultErrorHandler((consumerRecord, e) -> {
            // send to DLQ for example
        }, new FixedBackOff(1000, 4)));

        return factory;
    }

    @Bean("autoCommitConsumerFactory")
    public ConsumerFactory<?, ?> autoCommitConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetSetting);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaDemoListener demoListener() {
        return new KafkaDemoListener();
    }
}
