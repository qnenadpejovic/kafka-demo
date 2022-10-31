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
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedManualImmediateAck() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertEquals(2, messages.size());
        Assertions.assertEquals("msg1", messages.get(0));
        Assertions.assertEquals("msg2", messages.get(1));

    }

    // TODO: MANUAL_IMMEDIATE acknowledge without calling acknowledge
    // TODO: AUTO_COMMIT test commit interval
    // TODO: producer breaks
    // TODO: make separate branches
    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedManualImmediateNoAck() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg3");
        kafkaDemoProducer.sendMessage("msg4");
        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(4, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        long countMsg3 = messages.stream().filter(msg -> msg.equals("msg3")).count();
        long countMsg4 = messages.stream().filter(msg -> msg.equals("msg4")).count();

        Assertions.assertNotEquals(1, countMsg1);
        Assertions.assertNotEquals(1, countMsg2);
        Assertions.assertNotEquals(1, countMsg3);
        Assertions.assertNotEquals(1, countMsg4);
    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedRecord() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(2, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        Assertions.assertEquals(1, countMsg1);
        Assertions.assertNotEquals(1, countMsg2);
    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedBatch() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg4");
        kafkaDemoProducer.sendMessage("msg3");
        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(4, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        long countMsg3  = messages.stream().filter(msg -> msg.equals("msg3")).count();
        long countMsg4 = messages.stream().filter(msg -> msg.equals("msg4")).count();
        Assertions.assertEquals(1, countMsg3);
        Assertions.assertEquals(1, countMsg4);
        Assertions.assertNotEquals(1, countMsg1);
        Assertions.assertNotEquals(1, countMsg2);
    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedCount() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg3");
        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(3, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        long countMsg3 = messages.stream().filter(msg -> msg.equals("msg3")).count();

        Assertions.assertNotEquals(1, countMsg2);
        Assertions.assertEquals(1, countMsg1);
        Assertions.assertEquals(1, countMsg3);

    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithProducer_thenMessageReceivedTime() throws InterruptedException {

        kafkaDemoProducer.sendMessage("msg1");
        kafkaDemoProducer.sendMessage("msg3");
        kafkaDemoProducer.sendMessage("msg2");

        Thread.sleep(30000);
        List<String> messages = kafkaDemoListener.getMessages();

        Assertions.assertNotEquals(2, messages.size());
        long countMsg1 = messages.stream().filter(msg -> msg.equals("msg1")).count();
        long countMsg2 = messages.stream().filter(msg -> msg.equals("msg2")).count();
        long countMsg3 = messages.stream().filter(msg -> msg.equals("msg3")).count();


        Assertions.assertNotEquals(1, countMsg2);
        Assertions.assertEquals(1, countMsg1);
        Assertions.assertEquals(1, countMsg3);

    }

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
