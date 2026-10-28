package com.mds.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Event published by the Inventory Service after processing an order.
 * Consumed by the Order API to update the in-memory order status store,
 * making the result available via GET /orders/{orderId}.
 *
 * Status semantics:
 *   RESERVED     — stock was available and has been reserved
 *   REJECTED     — insufficient stock; order cannot be fulfilled
 *   UNKNOWN_ITEM — the requested item does not exist in the inventory catalog
 */
public record InventoryResultEvent(
        String orderId,
        String itemId,
        Status status,
        String message,
        Instant timestamp) {

    public enum Status {
        RESERVED,
        REJECTED,
        UNKNOWN_ITEM
    }

    @JsonCreator
    public InventoryResultEvent(
            @JsonProperty("orderId")   String orderId,
            @JsonProperty("itemId")    String itemId,
            @JsonProperty("status")    Status status,
            @JsonProperty("message")   String message,
            @JsonProperty("timestamp") Instant timestamp) {
        this.orderId    = Objects.requireNonNull(orderId,  "orderId must not be null");
        this.itemId     = Objects.requireNonNull(itemId,   "itemId must not be null");
        this.status     = Objects.requireNonNull(status,   "status must not be null");
        this.message    = message != null ? message : "";
        this.timestamp  = timestamp != null ? timestamp : Instant.now();
    }

    public static InventoryResultEvent of(String orderId, String itemId,
                                          Status status, String message) {
        return new InventoryResultEvent(orderId, itemId, status, message, Instant.now());
    }

    @Override
    public String toString() {
        return "InventoryResultEvent{orderId='" + orderId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp + '}';
    }
}
