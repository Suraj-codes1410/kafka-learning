package com.javakafka.kafkaproducerexample.service;

import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class kafkaMessagePublisher {

    public kafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    private final KafkaTemplate<String,Object> kafkaTemplate ;

    public void sendMessageToTopic (String message){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("javatechie-demo-3", message);
        future.whenComplete((result,ex)->{
            if(ex==null){
                System.out.println("Sent message=[" + message +
                        "]with offset=[" + result.getRecordMetadata().offset()+"]");
            }else{
                System.out.println("Unable to send message=[" +
                        message +"]due to :" + ex.getMessage());
            }
        });
    }
}
