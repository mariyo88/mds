package com.mds.orderapi.service;

import com.mds.shared.model.OrderEvent;

/**
 * Contract for publishing order events to the message broker.
 * Decoupled via interface so the controller doesn't depend on Kafka directly,
 * and the implementation can be swapped or mocked in tests.
 */
public interface OrderPublisherService {

    /**
     * Publishes the given order event to the broker.
     *
     * @param event the order event to publish
     */
    void publish(OrderEvent event);
}
