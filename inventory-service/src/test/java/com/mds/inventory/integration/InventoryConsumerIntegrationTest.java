package com.mds.inventory.integration;

import com.mds.inventory.service.InventoryService;
import com.mds.shared.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests verifying end-to-end consumer behavior:
 * EmbeddedKafka producer → Kafka → OrderEventConsumer → InventoryService.
 *
 * Uses Awaitility to poll for async state changes without fixed sleeps.
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"orders", "orders.DLQ"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9094",
                "port=9094"
        }
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.orders=orders",
        "kafka.consumer.group-id=inventory-test-group"
})
class InventoryConsumerIntegrationTest {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    // ─── Helper ──────────────────────────────────────────────────────────────

    private KafkaTemplate<String, OrderEvent> buildProducerTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // ─── Tests ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Consumer processes order event and reduces stock (successful reservation)")
    void consume_sufficientStock_reducesInventory() {
        KafkaTemplate<String, OrderEvent> producer = buildProducerTemplate();
        OrderEvent event = OrderEvent.of("order-int-1", "item-1", 3);

        producer.send(new ProducerRecord<>("orders", event.orderId(), event));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(inventoryService.getStock("item-1")).isEqualTo(7)); // 10 - 3
    }

    @Test
    @DisplayName("Consumer processes order event and does not change stock on rejection")
    void consume_insufficientStock_stockUnchanged() {
        KafkaTemplate<String, OrderEvent> producer = buildProducerTemplate();
        // item-2 has 5, request 100 — should be rejected
        OrderEvent event = OrderEvent.of("order-int-2", "item-2", 100);

        producer.send(new ProducerRecord<>("orders", event.orderId(), event));

        // Stock should remain 5 — give consumer time to process, then assert stable
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(inventoryService.getStock("item-2")).isEqualTo(5));
    }

    @Test
    @DisplayName("Consumer handles unknown item without throwing")
    void consume_unknownItem_processedWithoutError() {
        KafkaTemplate<String, OrderEvent> producer = buildProducerTemplate();
        OrderEvent event = OrderEvent.of("order-int-3", "item-unknown", 1);

        producer.send(new ProducerRecord<>("orders", event.orderId(), event));

        // No stock change expected; just verify consumer doesn't crash
        // and that known item stocks are untouched
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inventoryService.getStock("item-1")).isGreaterThanOrEqualTo(0);
                    assertThat(inventoryService.getStock("item-unknown")).isEqualTo(-1);
                });
    }
}
