package com.mds.orderapi.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Incoming REST payload for POST /orders.
 */
public class OrderRequest {

    @NotBlank(message = "orderId must not be blank")
    private String orderId;

    @NotBlank(message = "itemId must not be blank")
    private String itemId;

    @Positive(message = "quantity must be a positive number")
    private int quantity;

    // Default constructor for Jackson
    public OrderRequest() {}

    public OrderRequest(String orderId, String itemId, int quantity) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.quantity = quantity;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "OrderRequest{orderId='" + orderId + "', itemId='" + itemId + "', quantity=" + quantity + '}';
    }
}
