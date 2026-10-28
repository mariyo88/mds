package com.mds.orderapi.config;

import com.mds.shared.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration with production-grade reliability settings:
 *
 * - enable.idempotence=true       : exactly-once delivery per partition (no duplicates on retry)
 * - acks=all                      : leader waits for all in-sync replicas to ack before confirming
 * - retries=3                     : retry transient failures automatically
 * - max.in.flight.requests=1      : required for strict ordering when idempotence is enabled
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, OrderEvent> orderEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Connection
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Reliability: idempotent producer — no duplicates on broker-side retries
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Reliability: wait for all in-sync replicas to acknowledge the write
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Reliability: retry up to 3 times on transient failures
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Required with idempotence: prevents out-of-order delivery during retries
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Do not embed type headers — keeps messages broker-agnostic and
        // avoids coupling the consumer to Spring's type resolution mechanism
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> kafkaTemplate(
            ProducerFactory<String, OrderEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
