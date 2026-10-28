package com.mds.orderapi.model;

/**
 * Response body for POST /orders.
 */
public class OrderResponse {

    private final String orderId;
    private final String status;
    private final String message;

    public OrderResponse(String orderId, String status, String message) {
        this.orderId = orderId;
        this.status = status;
        this.message = message;
    }

    public static OrderResponse accepted(String orderId) {
        return new OrderResponse(orderId, "ACCEPTED", "Order event published successfully");
    }

    public String getOrderId() {
        return orderId;
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
