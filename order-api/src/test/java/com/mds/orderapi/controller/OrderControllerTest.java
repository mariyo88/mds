package com.mds.orderapi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mds.orderapi.model.OrderRequest;
import com.mds.orderapi.service.OrderPublisherService;
import com.mds.shared.model.OrderEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderPublisherService publisherService;

    @Test
    @DisplayName("POST /orders with valid payload returns 202 Accepted")
    void validRequest_returns202() throws Exception {
        OrderRequest request = new OrderRequest("order-123", "item-1", 2);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("order-123"))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        verify(publisherService, times(1)).publish(any(OrderEvent.class));
    }

    @Test
    @DisplayName("POST /orders publishes event with correct data")
    void validRequest_publishesCorrectEvent() throws Exception {
        OrderRequest request = new OrderRequest("order-456", "item-2", 5);
        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        verify(publisherService).publish(captor.capture());
        OrderEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo("order-456");
        assertThat(published.getItemId()).isEqualTo("item-2");
        assertThat(published.getQuantity()).isEqualTo(5);
        assertThat(published.getTimestamp()).isNotNull();
    }

    @Test
    @DisplayName("POST /orders with missing orderId returns 400")
    void missingOrderId_returns400() throws Exception {
        String payload = "{\"itemId\":\"item-1\",\"quantity\":2}";

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"));

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders with blank orderId returns 400")
    void blankOrderId_returns400() throws Exception {
        OrderRequest request = new OrderRequest("", "item-1", 2);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.messages").isArray());

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders with zero quantity returns 400")
    void zeroQuantity_returns400() throws Exception {
        OrderRequest request = new OrderRequest("order-1", "item-1", 0);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"));

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders with negative quantity returns 400")
    void negativeQuantity_returns400() throws Exception {
        OrderRequest request = new OrderRequest("order-1", "item-1", -3);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders with missing itemId returns 400")
    void missingItemId_returns400() throws Exception {
        String payload = "{\"orderId\":\"order-1\",\"quantity\":2}";

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders with empty body returns 400")
    void emptyBody_returns400() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(publisherService);
    }

    @Test
    @DisplayName("POST /orders when publisher throws returns 500")
    void publisherThrows_returns500() throws Exception {
        doThrow(new RuntimeException("Kafka unavailable"))
                .when(publisherService).publish(any());

        OrderRequest request = new OrderRequest("order-err", "item-1", 1);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Internal Server Error"));
    }
}
