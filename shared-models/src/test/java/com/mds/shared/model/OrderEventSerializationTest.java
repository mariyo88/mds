package com.mds.shared.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderEventSerializationTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    @DisplayName("OrderEvent serializes to JSON and deserializes back correctly")
    void shouldSerializeAndDeserialize() throws Exception {
        Instant now = Instant.parse("2024-01-15T10:30:00Z");
        OrderEvent original = new OrderEvent("order-123", "item-1", 3, now);

        String json = objectMapper.writeValueAsString(original);
        OrderEvent deserialized = objectMapper.readValue(json, OrderEvent.class);

        assertThat(deserialized.getOrderId()).isEqualTo("order-123");
        assertThat(deserialized.getItemId()).isEqualTo("item-1");
        assertThat(deserialized.getQuantity()).isEqualTo(3);
        assertThat(deserialized.getTimestamp()).isEqualTo(now);
    }

    @Test
    @DisplayName("Serialized JSON contains expected fields")
    void shouldContainExpectedJsonFields() throws Exception {
        OrderEvent event = OrderEvent.of("order-456", "item-2", 5);

        String json = objectMapper.writeValueAsString(event);

        assertThat(json).contains("\"orderId\":\"order-456\"");
        assertThat(json).contains("\"itemId\":\"item-2\"");
        assertThat(json).contains("\"quantity\":5");
        assertThat(json).contains("\"timestamp\"");
    }

    @Test
    @DisplayName("OrderEvent factory method sets timestamp to now")
    void ofFactoryMethodSetsTimestamp() {
        Instant before = Instant.now();
        OrderEvent event = OrderEvent.of("order-789", "item-3", 2);
        Instant after = Instant.now();

        assertThat(event.getTimestamp()).isBetween(before, after);
    }

    @Test
    @DisplayName("OrderEvent rejects null orderId")
    void shouldRejectNullOrderId() {
        assertThatThrownBy(() -> new OrderEvent(null, "item-1", 1, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("orderId");
    }

    @Test
    @DisplayName("OrderEvent rejects null itemId")
    void shouldRejectNullItemId() {
        assertThatThrownBy(() -> new OrderEvent("order-1", null, 1, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("itemId");
    }

    @Test
    @DisplayName("OrderEvent rejects non-positive quantity")
    void shouldRejectNonPositiveQuantity() {
        assertThatThrownBy(() -> new OrderEvent("order-1", "item-1", 0, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quantity");
    }

    @Test
    @DisplayName("OrderEvent deserializes from minimal JSON with null timestamp")
    void shouldDeserializeWithNullTimestamp() throws Exception {
        String json = "{\"orderId\":\"order-1\",\"itemId\":\"item-1\",\"quantity\":2,\"timestamp\":null}";
        OrderEvent event = objectMapper.readValue(json, OrderEvent.class);

        assertThat(event.getTimestamp()).isNotNull();
    }
}
