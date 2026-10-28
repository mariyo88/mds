package com.mds.inventory.consumer;

import com.mds.inventory.model.ReservationResult;
import com.mds.inventory.service.InventoryService;
import com.mds.shared.model.InventoryResultEvent;
import com.mds.shared.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for order events.
 * <p>
 * Processing contract:
 *  1. Receive message from the "orders" topic
 *  2. Delegate to InventoryService for stock reservation
 *  3. Publish an InventoryResultEvent to the "inventory-results" topic
 *     so the Order API can update its in-memory order status store
 *  4. Log the outcome
 *  5. Acknowledge the offset ONLY after successful processing
 *     (manual ack mode prevents message loss on crash)
 * <p>
 * If processing throws an unexpected exception, the DefaultErrorHandler
 * will retry with exponential backoff and eventually route to orders.DLQ.
 * Business-logic rejections (insufficient stock, unknown item) are NOT
 * exceptions — they are handled outcomes and the offset is still committed.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final InventoryService inventoryService;
    private final KafkaTemplate<String, InventoryResultEvent> resultKafkaTemplate;
    private final String inventoryResultsTopic;

    public OrderEventConsumer(
            InventoryService inventoryService,
            KafkaTemplate<String, InventoryResultEvent> resultKafkaTemplate,
            @Value("${kafka.topic.inventory-results}") String inventoryResultsTopic) {
        this.inventoryService = inventoryService;
        this.resultKafkaTemplate = resultKafkaTemplate;
        this.inventoryResultsTopic = inventoryResultsTopic;
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
                event.orderId(), event.itemId(), event.quantity(),
                topic, partition, offset);

        ReservationResult result = inventoryService.reserve(event);

        logResult(result);
        publishResult(result);

        // Commit offset — business outcomes (REJECTED, UNKNOWN_ITEM) are
        // valid terminal states and should not trigger a retry.
        acknowledgment.acknowledge();

        log.debug("Offset committed: topic={}, partition={}, offset={}",
                topic, partition, offset);
    }

    /**
     * Maps ReservationResult to InventoryResultEvent and publishes it to the
     * inventory-results topic. Uses orderId as the partition key to preserve
     * per-order ordering, consistent with the upstream orders topic.
     */
    private void publishResult(ReservationResult result) {
        InventoryResultEvent.Status status = switch (result.getStatus()) {
            case RESERVED     -> InventoryResultEvent.Status.RESERVED;
            case REJECTED     -> InventoryResultEvent.Status.REJECTED;
            case UNKNOWN_ITEM -> InventoryResultEvent.Status.UNKNOWN_ITEM;
        };

        InventoryResultEvent resultEvent = InventoryResultEvent.of(
                result.getOrderId(),
                result.getItemId(),
                status,
                result.getMessage());

        resultKafkaTemplate
                .send(inventoryResultsTopic, result.getOrderId(), resultEvent)
                .whenComplete((sendResult, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish InventoryResultEvent: orderId={}, error={}",
                                result.getOrderId(), ex.getMessage(), ex);
                    } else {
                        log.debug("InventoryResultEvent published: orderId={}, topic={}, partition={}, offset={}",
                                result.getOrderId(),
                                sendResult.getRecordMetadata().topic(),
                                sendResult.getRecordMetadata().partition(),
                                sendResult.getRecordMetadata().offset());
                    }
                });
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
