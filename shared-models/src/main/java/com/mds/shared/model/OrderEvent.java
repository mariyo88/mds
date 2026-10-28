package com.mds.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an order event published to Kafka by the Order API
 * and consumed by the Inventory Service.
 *
 * This class is immutable and shared between both services via the
 * shared-models module to ensure schema consistency.
 */
public class OrderEvent {

    private final String orderId;
    private final String itemId;
    private final int quantity;
    private final Instant timestamp;

    @JsonCreator
    public OrderEvent(
            @JsonProperty("orderId") String orderId,
            @JsonProperty("itemId") String itemId,
            @JsonProperty("quantity") int quantity,
            @JsonProperty("timestamp") Instant timestamp) {
        this.orderId = Objects.requireNonNull(orderId, "orderId must not be null");
        this.itemId = Objects.requireNonNull(itemId, "itemId must not be null");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got: " + quantity);
        }
        this.quantity = quantity;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    /**
     * Convenience factory method — sets timestamp to now.
     */
    public static OrderEvent of(String orderId, String itemId, int quantity) {
        return new OrderEvent(orderId, itemId, quantity, Instant.now());
    }

    public String getOrderId() {
        return orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public int getQuantity() {
        return quantity;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderEvent that)) return false;
        return quantity == that.quantity
                && Objects.equals(orderId, that.orderId)
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, itemId, quantity, timestamp);
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", quantity=" + quantity +
                ", timestamp=" + timestamp +
                '}';
    }
}
