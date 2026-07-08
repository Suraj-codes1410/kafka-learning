package com.javakafka.kafkaconsumerexample.consumer;

import com.javakafka.kafkacommon.dto.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserDltConsumer {

    @KafkaListener(
            topics = "user-registration-topic-dlt",
            groupId = "registration-group"
    )
    public void consume(User user){

        System.out.println("========= DLT =========");
        System.out.println(user);
    }
}