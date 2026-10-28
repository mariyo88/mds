package com.mds.orderapi.model;

import com.mds.orderapi.store.OrderStatusStore.OrderStatusEntry;

import java.time.Instant;

/**
 * Response body for GET /orders/{orderId}.
 *
 * Reflects the current processing outcome as recorded by the
 * InventoryResultConsumer. The status field maps directly to
 * InventoryResultEvent.Status values:
 *   RESERVED     — stock reserved, order will be fulfilled
 *   REJECTED     — insufficient stock, order cannot be fulfilled
 *   UNKNOWN_ITEM — item does not exist in the inventory catalog
 *   PENDING      — event not yet processed by the inventory service
 */
public record OrderStatusResponse(
        String orderId,
        String itemId,
        String status,
        String message,
        Instant updatedAt) {

    public static OrderStatusResponse from(OrderStatusEntry entry) {
        return new OrderStatusResponse(
                entry.orderId(),
                entry.itemId(),
                entry.status().name(),
                entry.message(),
                entry.updatedAt());
    }

    public static OrderStatusResponse pending(String orderId) {
        return new OrderStatusResponse(
                orderId,
                null,
                "PENDING",
                "Order received and queued for processing. Check back shortly.",
                null);
    }
}
