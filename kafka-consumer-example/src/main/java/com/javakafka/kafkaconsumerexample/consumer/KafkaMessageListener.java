package com.javakafka.kafkaconsumerexample.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    // type of the method would be same as that of the producer method because this method would consume that event
   // In real World scenarios one should write multiple consumer code for same topic
    // If there are a lot of messages from the producers there will be a lag (consumer doesnt consume the messages)
    @KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
    public void consume1(String message){
        log.info("consumer1 consume the message{}" , message);

    }@KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
    public void consume2(String message){
        log.info("consumer2 consume the message{}" , message);

    }@KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
    public void consume3(String message){
        log.info("consumer3 consume the message{}" , message);

    }@KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
    public void consume4(String message){
        log.info("consumer4 consume the message{}" , message);

    }
}
