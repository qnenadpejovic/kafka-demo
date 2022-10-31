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
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Autowired
    KafkaDemoProducer kafkaDemoProducer;
    @Autowired
    KafkaDemoListener kafkaDemoListener;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedManualAutoAck() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");
        kafkaDemoProducer.sendMessage("msg3");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertEquals(3, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        long countMsg3 = messages.stream().filter(msg -> msg.equals("msg3")).count();

        Assertions.assertEquals(1, countMsg1);
        Assertions.assertEquals(1, countMsg2);
        Assertions.assertEquals(1, countMsg3);
    }
}
