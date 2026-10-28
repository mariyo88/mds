package com.mds.orderapi.controller;

import com.mds.orderapi.model.OrderRequest;
import com.mds.orderapi.model.OrderResponse;
import com.mds.orderapi.model.OrderStatusResponse;
import com.mds.orderapi.service.OrderPublisherService;
import com.mds.orderapi.store.OrderStatusStore;
import com.mds.shared.model.OrderEvent;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for the Order API.
 * Accepts incoming order requests, validates them, and delegates
 * to the publisher service to emit an event to the broker.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderPublisherService publisherService;
    private final OrderStatusStore orderStatusStore;

    public OrderController(OrderPublisherService publisherService,
                           OrderStatusStore orderStatusStore) {
        this.publisherService = publisherService;
        this.orderStatusStore = orderStatusStore;
    }

    /**
     * POST /orders
     *
     * Validates the request and publishes an OrderEvent to Kafka.
     * Returns 202 Accepted to indicate the order has been received
     * and queued for processing — not that it has been fulfilled yet.
     */
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody OrderRequest request) {
        log.info("Received order request: orderId={}, itemId={}, quantity={}",
                request.getOrderId(), request.getItemId(), request.getQuantity());

        OrderEvent event = OrderEvent.of(request.getOrderId(), request.getItemId(), request.getQuantity());
        publisherService.publish(event);

        log.info("Order event queued for publishing: orderId={}", request.getOrderId());
        return ResponseEntity.accepted().body(OrderResponse.accepted(request.getOrderId()));
    }

    /**
     * GET /orders/{orderId}
     *
     * Returns the current processing status of an order as recorded by the
     * InventoryResultConsumer after the inventory-service processes the event.
     *
     * Possible status values:
     *   PENDING      — event not yet processed (inventory-service hasn't responded)
     *   RESERVED     — stock reserved, order will be fulfilled
     *   REJECTED     — insufficient stock, order cannot be fulfilled
     *   UNKNOWN_ITEM — requested item does not exist in the inventory catalog
     *
     * Returns 200 in all cases where the orderId is known (including PENDING),
     * and 404 if the orderId was never submitted via POST /orders.
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderStatusResponse> getOrderStatus(@PathVariable("orderId") String orderId) {
        log.debug("Status query for orderId={}", orderId);

        return orderStatusStore.get(orderId)
                .map(entry -> {
                    log.debug("Order status found: orderId={}, status={}", orderId, entry.status());
                    return ResponseEntity.ok(OrderStatusResponse.from(entry));
                })
                .orElseGet(() -> {
                    log.debug("Order status not yet available: orderId={}", orderId);
                    return ResponseEntity.ok(OrderStatusResponse.pending(orderId));
                });
    }
}
