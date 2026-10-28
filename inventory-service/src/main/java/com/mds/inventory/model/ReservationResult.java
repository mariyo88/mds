package com.mds.inventory.model;

/**
 * Outcome of an inventory reservation attempt.
 */
public class ReservationResult {

    public enum Status {
        /** Stock was available and has been reserved. */
        RESERVED,
        /** Insufficient stock — order rejected. */
        REJECTED,
        /** Item does not exist in the inventory catalog. */
        UNKNOWN_ITEM
    }

    private final String orderId;
    private final String itemId;
    private final int requestedQuantity;
    private final int availableQuantityAtTime;
    private final Status status;
    private final String message;

    private ReservationResult(String orderId, String itemId, int requestedQuantity,
                               int availableQuantityAtTime, Status status, String message) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.requestedQuantity = requestedQuantity;
        this.availableQuantityAtTime = availableQuantityAtTime;
        this.status = status;
        this.message = message;
    }

    public static ReservationResult reserved(String orderId, String itemId,
                                              int requested, int availableBefore) {
        return new ReservationResult(orderId, itemId, requested, availableBefore,
                Status.RESERVED,
                String.format("Reserved %d unit(s) of '%s' for order '%s'",
                        requested, itemId, orderId));
    }

    public static ReservationResult rejected(String orderId, String itemId,
                                              int requested, int available) {
        return new ReservationResult(orderId, itemId, requested, available,
                Status.REJECTED,
                String.format("Insufficient stock for '%s': requested=%d, available=%d",
                        itemId, requested, available));
    }

    public static ReservationResult unknownItem(String orderId, String itemId, int requested) {
        return new ReservationResult(orderId, itemId, requested, 0,
                Status.UNKNOWN_ITEM,
                String.format("Item '%s' not found in inventory catalog", itemId));
    }

    public String getOrderId()              { return orderId; }
    public String getItemId()               { return itemId; }
    public int getRequestedQuantity()       { return requestedQuantity; }
    public int getAvailableQuantityAtTime() { return availableQuantityAtTime; }
    public Status getStatus()               { return status; }
    public String getMessage()              { return message; }
    public boolean isSuccessful()           { return status == Status.RESERVED; }

    @Override
    public String toString() {
        return "ReservationResult{orderId='" + orderId + "', itemId='" + itemId +
                "', status=" + status + ", message='" + message + "'}";
    }
}
