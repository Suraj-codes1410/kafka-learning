//package com.javakafka.kafkaconsumerexample.config;
//
//import com.javakafka.kafkacommon.dto.Customer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.support.serializer.JsonDeserializer;
//
//import java.util.HashMap;
//import java.util.Map;
//
//// For customization use Java based Config instead of aplication.yml file
//
//
//@Configuration
//public class KafkaConsumerConfig {
//
//    @Bean
//    public Map<String, Object> consumerConfig() {
//
//        Map<String, Object> props = new HashMap<>();
//
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "jt-group");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        return props;
//    }
//
//    @Bean
//    public ConsumerFactory<String, Customer> consumerFactory() {
//
//        JsonDeserializer<Customer> deserializer =
//                new JsonDeserializer<>(Customer.class);
//
//        deserializer.addTrustedPackages("*");
//
//        return new DefaultKafkaConsumerFactory<>(
//                consumerConfig(),
//                new StringDeserializer(),
//                deserializer
//        );
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Customer> kafkaListenerContainerFactory() {
//
//        ConcurrentKafkaListenerContainerFactory<String, Customer> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//
//        factory.setConsumerFactory(consumerFactory());
//
//        return factory;
//    }
//}
