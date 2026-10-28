package com.mds.orderapi.service;

import com.mds.shared.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka-backed implementation of {@link OrderPublisherService}.
 *
 * Uses orderId as the Kafka message key so that all events for the same
 * order are routed to the same partition, preserving per-order ordering.
 *
 * The send is asynchronous — the method returns as soon as the message
 * is handed off to the producer buffer. Callbacks log the outcome
 * (ack from broker or failure) without blocking the HTTP thread.
 */
@Service
public class KafkaOrderPublisherService implements OrderPublisherService {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderPublisherService.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String ordersTopic;

    public KafkaOrderPublisherService(
            KafkaTemplate<String, OrderEvent> kafkaTemplate,
            @Value("${kafka.topic.orders}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    @Override
    public void publish(OrderEvent event) {
        log.debug("Publishing OrderEvent to topic '{}': orderId={}, itemId={}, quantity={}",
                ordersTopic, event.getOrderId(), event.getItemId(), event.getQuantity());

        // Use orderId as the partition key for per-order message ordering
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(ordersTopic, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish OrderEvent: orderId={}, itemId={}, error={}",
                        event.getOrderId(), event.getItemId(), ex.getMessage(), ex);
            } else {
                log.info("OrderEvent published: orderId={}, topic={}, partition={}, offset={}",
                        event.getOrderId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
