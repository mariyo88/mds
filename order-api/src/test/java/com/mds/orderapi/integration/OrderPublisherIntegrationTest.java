package com.mds.orderapi.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mds.orderapi.model.OrderRequest;
import com.mds.shared.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration test verifying the full flow:
 * HTTP POST /orders → Kafka producer → EmbeddedKafka → consumer receives the event.
 */
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext
@EmbeddedKafka(
        partitions = 3,
        topics = {"orders"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9093",
                "port=9093"
        }
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.orders=orders"
})
class OrderPublisherIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private KafkaMessageListenerContainer<String, OrderEvent> container;
    private BlockingQueue<ConsumerRecord<String, OrderEvent>> receivedRecords;

    @BeforeEach
    void setUp() {
        receivedRecords = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.mds.shared.model");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());

        DefaultKafkaConsumerFactory<String, OrderEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProps = new ContainerProperties("orders");
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.setupMessageListener((MessageListener<String, OrderEvent>) receivedRecords::add);
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterEach
    void tearDown() {
        container.stop();
    }

    @Test
    @DisplayName("POST /orders publishes an OrderEvent to Kafka with correct data")
    void postOrder_publishesEventToKafka() throws Exception {
        OrderRequest request = new OrderRequest("order-it-001", "item-1", 3);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        ConsumerRecord<String, OrderEvent> record = receivedRecords.poll(10, TimeUnit.SECONDS);

        assertThat(record).as("Expected a Kafka record but received none within timeout").isNotNull();
        assertThat(record.key()).isEqualTo("order-it-001");
        assertThat(record.value().getOrderId()).isEqualTo("order-it-001");
        assertThat(record.value().getItemId()).isEqualTo("item-1");
        assertThat(record.value().getQuantity()).isEqualTo(3);
        assertThat(record.value().getTimestamp()).isNotNull();
    }

    @Test
    @DisplayName("POST /orders uses orderId as Kafka message key for partition routing")
    void postOrder_usesOrderIdAsKey() throws Exception {
        OrderRequest request = new OrderRequest("order-key-test", "item-2", 5);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted());

        ConsumerRecord<String, OrderEvent> record = receivedRecords.poll(10, TimeUnit.SECONDS);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("order-key-test");
        assertThat(record.topic()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Multiple POST /orders each produce a separate Kafka event")
    void multipleOrders_eachProduceSeparateEvents() throws Exception {
        OrderRequest req1 = new OrderRequest("order-multi-1", "item-1", 1);
        OrderRequest req2 = new OrderRequest("order-multi-2", "item-2", 2);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req1)))
                .andExpect(status().isAccepted());

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req2)))
                .andExpect(status().isAccepted());

        ConsumerRecord<String, OrderEvent> record1 = receivedRecords.poll(10, TimeUnit.SECONDS);
        ConsumerRecord<String, OrderEvent> record2 = receivedRecords.poll(10, TimeUnit.SECONDS);

        assertThat(record1).isNotNull();
        assertThat(record2).isNotNull();

        // Both events should be present, order may vary
        assertThat(record1.value().getOrderId()).isNotEqualTo(record2.value().getOrderId());
    }
}
