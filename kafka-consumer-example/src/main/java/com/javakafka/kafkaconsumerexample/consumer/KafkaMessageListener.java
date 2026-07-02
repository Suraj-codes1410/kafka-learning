package com.javakafka.kafkaconsumerexample.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    // type of the method would be same as that of the producer method because this method would consume that event

    @KafkaListener(topics="javatechie-demo-3",groupId = "jt-group-1")
    public void consume(String message){
        log.info("consumer consume the message{}" , message);

    }
}
