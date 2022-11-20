package com.htec.demo;

import com.htec.demo.listner.KafkaDemoListener;
import com.htec.demo.producer.KafkaDemoProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = "demo-topic", brokerProperties = {})
public class EmbeddedKafkaTest {

    @Autowired
    KafkaDemoProducer kafkaDemoProducer;
    @Autowired
    KafkaDemoListener kafkaDemoListener;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedManual() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(2, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();

        Assertions.assertNotEquals(1, countMsg2);
        Assertions.assertNotEquals(1, countMsg1);

    }
}
