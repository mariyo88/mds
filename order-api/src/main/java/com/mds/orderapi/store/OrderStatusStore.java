package com.mds.orderapi.store;

import com.mds.shared.model.InventoryResultEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory store for order status entries.
 *
 * Entries are written by the Kafka consumer when an InventoryResultEvent
 * arrives from the inventory-results topic, and read by GET /orders/{orderId}.
 *
 * Lifecycle note: this store is not persisted — a service restart clears all
 * entries. In production, this would be backed by a database or a distributed
 * cache (e.g. Redis) to survive restarts and scale horizontally.
 */
@Component
public class OrderStatusStore {

    /**
     * Snapshot of a single order's inventory processing outcome.
     */
    public record OrderStatusEntry(
            String orderId,
            String itemId,
            InventoryResultEvent.Status status,
            String message,
            Instant updatedAt) {

        public static OrderStatusEntry from(InventoryResultEvent event) {
            return new OrderStatusEntry(
                    event.orderId(),
                    event.itemId(),
                    event.status(),
                    event.message(),
                    event.timestamp());
        }
    }

    private final ConcurrentHashMap<String, OrderStatusEntry> store = new ConcurrentHashMap<>();

    /**
     * Stores or updates the status entry for the given order.
     * Called by the Kafka consumer on each InventoryResultEvent.
     */
    public void put(InventoryResultEvent event) {
        store.put(event.orderId(), OrderStatusEntry.from(event));
    }

    /**
     * Returns the status entry for the given orderId, or empty if not yet processed.
     */
    public Optional<OrderStatusEntry> get(String orderId) {
        return Optional.ofNullable(store.get(orderId));
    }
}
