package com.mds.inventory.consumer;

import com.mds.inventory.model.ReservationResult;
import com.mds.inventory.service.InventoryService;
import com.mds.shared.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for order events.
 *
 * Processing contract:
 *  1. Receive message from the "orders" topic
 *  2. Delegate to InventoryService for stock reservation
 *  3. Log the outcome
 *  4. Acknowledge the offset ONLY after successful processing
 *     (manual ack mode prevents message loss on crash)
 *
 * If processing throws an unexpected exception, the DefaultErrorHandler
 * will retry with exponential backoff and eventually route to orders.DLQ.
 * Business-logic rejections (insufficient stock, unknown item) are NOT
 * exceptions — they are handled outcomes and the offset is still committed.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final InventoryService inventoryService;

    public OrderEventConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "${kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderEvent(
            OrderEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received OrderEvent: orderId={}, itemId={}, quantity={}, topic={}, partition={}, offset={}",
                event.getOrderId(), event.getItemId(), event.getQuantity(),
                topic, partition, offset);

        ReservationResult result = inventoryService.reserve(event);

        logResult(result);

        // Commit offset — business outcomes (REJECTED, UNKNOWN_ITEM) are
        // valid terminal states and should not trigger a retry.
        acknowledgment.acknowledge();

        log.debug("Offset committed: topic={}, partition={}, offset={}",
                topic, partition, offset);
    }

    private void logResult(ReservationResult result) {
        switch (result.getStatus()) {
            case RESERVED -> log.info(
                    "INVENTORY_RESERVED: orderId={}, itemId={}, quantity={}, message={}",
                    result.getOrderId(), result.getItemId(),
                    result.getRequestedQuantity(), result.getMessage());

            case REJECTED -> log.warn(
                    "INVENTORY_REJECTED: orderId={}, itemId={}, requested={}, available={}, message={}",
                    result.getOrderId(), result.getItemId(),
                    result.getRequestedQuantity(), result.getAvailableQuantityAtTime(),
                    result.getMessage());

            case UNKNOWN_ITEM -> log.warn(
                    "INVENTORY_UNKNOWN_ITEM: orderId={}, itemId={}, message={}",
                    result.getOrderId(), result.getItemId(), result.getMessage());
        }
    }
}
