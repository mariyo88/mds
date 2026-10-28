package com.mds.orderapi.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics managed by this service.
 * Spring Kafka's KafkaAdmin will auto-create topics on startup if they don't exist.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
