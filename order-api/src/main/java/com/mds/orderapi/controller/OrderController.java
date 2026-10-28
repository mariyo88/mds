package com.mds.orderapi.controller;

import com.mds.orderapi.model.OrderRequest;
import com.mds.orderapi.model.OrderResponse;
import com.mds.orderapi.service.OrderPublisherService;
import com.mds.shared.model.OrderEvent;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
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

    public OrderController(OrderPublisherService publisherService) {
        this.publisherService = publisherService;
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
}
