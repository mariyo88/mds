package com.mds.inventory.service;

import com.mds.inventory.model.ReservationResult;
import com.mds.shared.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory inventory store with thread-safe reservation logic.
 *
 * Initial stock is hardcoded as per task requirements:
 *   item-1 → 10 units
 *   item-2 →  5 units
 *   item-3 →  0 units
 *
 * Reservation is atomic via synchronized block on the item entry to
 * prevent race conditions under concurrent consumer processing.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // Thread-safe map: itemId → current stock level
    private final ConcurrentHashMap<String, Integer> stock;

    public InventoryService() {
        Map<String, Integer> initialStock = new HashMap<>();
        initialStock.put("item-1", 10);
        initialStock.put("item-2", 5);
        initialStock.put("item-3", 0);
        this.stock = new ConcurrentHashMap<>(initialStock);
    }

    /**
     * Attempts to reserve the requested quantity for the given order.
     * The operation is atomic per item — concurrent reservations for the
     * same item are serialized to prevent overselling.
     *
     * @param event the order event containing itemId and quantity
     * @return a {@link ReservationResult} describing the outcome
     */
    public ReservationResult reserve(OrderEvent event) {
        String itemId = event.getItemId();
        String orderId = event.getOrderId();
        int requested = event.getQuantity();

        if (!stock.containsKey(itemId)) {
            log.warn("Reservation attempt for unknown item: orderId={}, itemId={}", orderId, itemId);
            return ReservationResult.unknownItem(orderId, itemId, requested);
        }

        // Synchronize on the item key's intern to serialize concurrent reservations
        // for the same item while allowing parallelism across different items.
        synchronized (itemId.intern()) {
            int available = stock.get(itemId);

            if (available < requested) {
                log.warn("Insufficient stock: orderId={}, itemId={}, requested={}, available={}",
                        orderId, itemId, requested, available);
                return ReservationResult.rejected(orderId, itemId, requested, available);
            }

            stock.put(itemId, available - requested);
            log.info("Stock reserved: orderId={}, itemId={}, reserved={}, remaining={}",
                    orderId, itemId, requested, available - requested);
            return ReservationResult.reserved(orderId, itemId, requested, available);
        }
    }

    /**
     * Returns a read-only snapshot of current stock levels.
     * Useful for health checks and observability endpoints.
     */
    public Map<String, Integer> getStockSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(stock));
    }

    /**
     * Returns the current stock level for a specific item.
     * Returns -1 if the item is not in the catalog.
     */
    public int getStock(String itemId) {
        return stock.getOrDefault(itemId, -1);
    }
}
