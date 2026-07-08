package com.javakafka.kafkaconsumerexample.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javakafka.kafkacommon.dto.Customer;
import com.javakafka.kafkacommon.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    //
    //This met


//    @KafkaListener(topics = "java-partition-learn", groupId = "jt-group")
//    public void consume(Customer customer) {
//
//        log.info("Received : {}", customer);
//
//    }



    // type of the method would be same as that of the producer method because this method would consume that event
    // In real World scenarios one should write multiple consumer code for same topic
    // If there are a lot of messages from the producers there will be a lag (consumer doesnt consume the messages)

//     @KafkaListener(topics="java-partition-learn",groupId = "jt-group",topicPartitions = {@TopicPartition(topic="java-partition-learn",partitions = {"2"})})
//    public void consume1(String message){
//        log.info("consumer1 consume the message{}" ,message );
//    }
//    @KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
//    public void consume2(String message){
//        log.info("consumer2 consume the message{}" , message);
//
//    }
//    @KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
//    public void consume3(String message){
//        log.info("consumer3 consume the message{}" , message);
//
//    }
//    @KafkaListener(topics="javatechie-demo1",groupId = "jt-group")
//    public void consume4(String message){
//        log.info("consumer4 consume the message{}" , message);
//
//    }


//    @KafkaListener(topics = "{app.topic.name}",groupId = "javatechie-group")
//    public void consumeEvents(User user , @Header(KafkaHeaders.RECEIVED_TOPIC) String topic , @Header(KafkaHeaders.OFFSET) long offset){
//         try{
//         log.info("Recieved : {} from {} offset {}",new ObjectMapper().writeValueAsString(user));
//             List<String> restrictedIpList = Stream.of("32.241.244.236","15.55.49.164","81.1.995.246").toList();
//             if(restrictedIpList.contains(user.getIpAddress())){
//                 throw new RuntimeException("Invalid IP Address recieved !");
//             }
//
//         } catch (JsonProcessingException e) {
//             throw new RuntimeException(e);
//         }
//    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 5000)
    )
    @KafkaListener(
            topics = "user-registration-topic",
            groupId = "registration-group"
    )
    public void consume(User user){

        System.out.println("Received : " + user);

        throw new RuntimeException("SMTP Server Down");
    }


}
