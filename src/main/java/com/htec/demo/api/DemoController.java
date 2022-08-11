package com.htec.demo.api;

import com.htec.demo.producer.KafkaDemoProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {

    @Autowired
    private KafkaDemoProducer kafkaDemoProducer;

    @PostMapping
    public ResponseEntity demoProducerTest(@RequestBody String message) {
        kafkaDemoProducer.sendMessage(message);
        return ResponseEntity.ok().build();
    }
}
