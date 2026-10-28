package com.mds.orderapi.consumer;

import com.mds.orderapi.store.OrderStatusStore;
import com.mds.shared.model.InventoryResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for inventory result events.
 *
 * Listens on the inventory-results topic and updates the in-memory
 * OrderStatusStore so that GET /orders/{orderId} can return the
 * current processing outcome to the client.
 *
 * Offset is committed only after the store has been updated (MANUAL_IMMEDIATE),
 * ensuring no status update is lost on consumer restart.
 */
@Component
public class InventoryResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryResultConsumer.class);

    private final OrderStatusStore orderStatusStore;

    public InventoryResultConsumer(OrderStatusStore orderStatusStore) {
        this.orderStatusStore = orderStatusStore;
    }

    @KafkaListener(
            topics = "${kafka.topic.inventory-results}",
            groupId = "${kafka.consumer.group-id}",
            containerFactory = "inventoryResultListenerContainerFactory"
    )
    public void onInventoryResult(
            InventoryResultEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received InventoryResultEvent: orderId={}, itemId={}, status={}, topic={}, partition={}, offset={}",
                event.orderId(), event.itemId(), event.status(),
                topic, partition, offset);

        orderStatusStore.put(event);

        acknowledgment.acknowledge();

        log.debug("Order status stored and offset committed: orderId={}, status={}",
                event.orderId(), event.status());
    }
}
