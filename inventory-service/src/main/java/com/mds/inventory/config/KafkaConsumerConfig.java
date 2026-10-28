package com.mds.inventory.config;

import com.mds.shared.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration with production-grade reliability:
 * <p>
 * - Manual offset commit (MANUAL_IMMEDIATE): offset is committed only after
 *   successful processing, preventing message loss on consumer crash.
 * <p>
 * - DefaultErrorHandler + ExponentialBackOff: retries failed messages
 *   up to 3 times (1s → 2s → 4s) before sending to the DLQ.
 * <p>
 * - DeadLetterPublishingRecoverer: exhausted messages are published to
 *   "orders.DLQ" for later inspection / replay without blocking the main topic.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    // ─── Consumer Factory ────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, OrderEvent> orderEventConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Start from the earliest unread offset when a new consumer group joins
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Disable auto-commit — we commit manually after successful processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Trust our shared model package; no Spring type headers were added by the producer
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.mds.shared.model");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ─── DLQ Producer Factory ────────────────────────────────────────────────

    /**
     * Separate producer used exclusively by the DeadLetterPublishingRecoverer.
     * Kept intentionally simple — DLQ messages are best-effort and do not need
     * the same idempotency guarantees as the main producer.
     */
    @Bean
    public ProducerFactory<String, Object> dlqProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate(
            ProducerFactory<String, Object> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }

    // ─── Error Handler with DLQ ──────────────────────────────────────────────

    /**
     * Recoverer that publishes failed messages to "{originalTopic}.DLQ".
     * For the "orders" topic this produces → "orders.DLQ".
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, Object> dlqKafkaTemplate) {
        return new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(
                        record.topic() + ".DLQ", 0));
    }

    /**
     * Error handler with exponential backoff:
     *   attempt 1 — immediately
     *   attempt 2 — after ~1 second
     *   attempt 3 — after ~2 seconds
     *   attempt 4 — after ~4 seconds  → then sent to DLQ
     * <p>
     * MaxAttempts = 4 means 1 initial + 3 retries.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);  // 3 retries after the initial attempt

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Log the exception on each retry attempt
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                org.slf4j.LoggerFactory
                        .getLogger(KafkaConsumerConfig.class)
                        .warn("Retry attempt {} for record: topic={}, partition={}, offset={}, error={}",
                                deliveryAttempt,
                                record.topic(), record.partition(), record.offset(),
                                ex.getMessage()));

        return handler;
    }

    // ─── Listener Container Factory ──────────────────────────────────────────

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> orderEventConsumerFactory,
            DefaultErrorHandler defaultErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(orderEventConsumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);

        // Manual immediate commit: acknowledge only after processing completes
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
