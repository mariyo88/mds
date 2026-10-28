package com.mds.inventory.service;

import com.mds.inventory.model.ReservationResult;
import com.mds.shared.model.OrderEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryServiceTest {

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService();
    }

    // ─── Successful reservation ──────────────────────────────────────────────

    @Test
    @DisplayName("Reservation succeeds when stock is sufficient")
    void reserve_sufficientStock_returnsReserved() {
        OrderEvent event = OrderEvent.of("order-1", "item-1", 3);

        ReservationResult result = inventoryService.reserve(event);

        assertThat(result.getStatus()).isEqualTo(ReservationResult.Status.RESERVED);
        assertThat(result.isSuccessful()).isTrue();
        assertThat(result.getOrderId()).isEqualTo("order-1");
        assertThat(result.getItemId()).isEqualTo("item-1");
        assertThat(result.getRequestedQuantity()).isEqualTo(3);
    }

    @Test
    @DisplayName("Stock decreases after successful reservation")
    void reserve_sufficientStock_decreasesStock() {
        OrderEvent event = OrderEvent.of("order-2", "item-1", 4);

        inventoryService.reserve(event);

        assertThat(inventoryService.getStock("item-1")).isEqualTo(6); // 10 - 4
    }

    @Test
    @DisplayName("Reservation succeeds when requesting exactly all available stock")
    void reserve_exactStock_returnsReserved() {
        OrderEvent event = OrderEvent.of("order-3", "item-1", 10);

        ReservationResult result = inventoryService.reserve(event);

        assertThat(result.getStatus()).isEqualTo(ReservationResult.Status.RESERVED);
        assertThat(inventoryService.getStock("item-1")).isEqualTo(0);
    }

    // ─── Rejected reservation ────────────────────────────────────────────────

    @Test
    @DisplayName("Reservation rejected when stock is insufficient")
    void reserve_insufficientStock_returnsRejected() {
        OrderEvent event = OrderEvent.of("order-4", "item-2", 10); // item-2 has only 5

        ReservationResult result = inventoryService.reserve(event);

        assertThat(result.getStatus()).isEqualTo(ReservationResult.Status.REJECTED);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getAvailableQuantityAtTime()).isEqualTo(5);
        assertThat(result.getRequestedQuantity()).isEqualTo(10);
    }

    @Test
    @DisplayName("Reservation rejected when stock is zero")
    void reserve_zeroStock_returnsRejected() {
        OrderEvent event = OrderEvent.of("order-5", "item-3", 1); // item-3 has 0

        ReservationResult result = inventoryService.reserve(event);

        assertThat(result.getStatus()).isEqualTo(ReservationResult.Status.REJECTED);
        assertThat(result.getAvailableQuantityAtTime()).isEqualTo(0);
    }

    @Test
    @DisplayName("Stock does not change on rejected reservation")
    void reserve_insufficientStock_stockUnchanged() {
        OrderEvent event = OrderEvent.of("order-6", "item-2", 100);

        inventoryService.reserve(event);

        assertThat(inventoryService.getStock("item-2")).isEqualTo(5); // unchanged
    }

    // ─── Unknown item ────────────────────────────────────────────────────────

    @Test
    @DisplayName("Reservation returns UNKNOWN_ITEM for unrecognized itemId")
    void reserve_unknownItem_returnsUnknownItem() {
        OrderEvent event = OrderEvent.of("order-7", "item-999", 1);

        ReservationResult result = inventoryService.reserve(event);

        assertThat(result.getStatus()).isEqualTo(ReservationResult.Status.UNKNOWN_ITEM);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getMessage()).contains("item-999");
    }

    // ─── Sequential reservations ─────────────────────────────────────────────

    @Test
    @DisplayName("Multiple sequential reservations deplete stock correctly")
    void reserve_sequential_depletes_stock() {
        inventoryService.reserve(OrderEvent.of("order-a", "item-1", 3));
        inventoryService.reserve(OrderEvent.of("order-b", "item-1", 3));
        inventoryService.reserve(OrderEvent.of("order-c", "item-1", 3));

        assertThat(inventoryService.getStock("item-1")).isEqualTo(1); // 10 - 9

        // Fourth reservation should be rejected (only 1 left, requesting 2)
        ReservationResult last = inventoryService.reserve(OrderEvent.of("order-d", "item-1", 2));
        assertThat(last.getStatus()).isEqualTo(ReservationResult.Status.REJECTED);
    }

    // ─── Stock snapshot ──────────────────────────────────────────────────────

    @Test
    @DisplayName("getStockSnapshot returns all initial items")
    void getStockSnapshot_returnsAllItems() {
        var snapshot = inventoryService.getStockSnapshot();

        assertThat(snapshot).containsKeys("item-1", "item-2", "item-3");
        assertThat(snapshot.get("item-1")).isEqualTo(10);
        assertThat(snapshot.get("item-2")).isEqualTo(5);
        assertThat(snapshot.get("item-3")).isEqualTo(0);
    }

    @Test
    @DisplayName("getStock returns -1 for unknown item")
    void getStock_unknownItem_returnsMinusOne() {
        assertThat(inventoryService.getStock("item-unknown")).isEqualTo(-1);
    }
}
