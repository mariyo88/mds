package com.mds.inventory.config;

import com.mds.shared.model.InventoryResultEvent;
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
 * Kafka producer configuration for the Inventory Service.
 *
 * Publishes {@link InventoryResultEvent} messages to the inventory-results topic
 * after each reservation attempt, allowing the Order API to update its
 * in-memory order status store.
 *
 * Reliability settings mirror the Order API producer:
 * - enable.idempotence=true  : no duplicates on broker-side retries
 * - acks=all                 : waits for all in-sync replicas
 * - retries=3                : retries transient failures
 * - max.in.flight.requests=1 : required for strict ordering with idempotence
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, InventoryResultEvent> inventoryResultProducerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // No Spring type headers — keeps messages broker-agnostic
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, InventoryResultEvent> inventoryResultKafkaTemplate(
            ProducerFactory<String, InventoryResultEvent> inventoryResultProducerFactory) {
        return new KafkaTemplate<>(inventoryResultProducerFactory);
    }
}
