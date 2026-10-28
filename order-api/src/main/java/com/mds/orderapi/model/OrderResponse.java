package com.mds.orderapi.model;

/**
 * Response body for POST /orders.
 */
public record OrderResponse(String orderId, String status, String message) {

    public static OrderResponse accepted(String orderId) {
        return new OrderResponse(orderId, "ACCEPTED", "Order event published successfully");
    }
}
