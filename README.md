# MDS — Event-Driven Order/Inventory System

A small event-driven system built with Java 21, Spring Boot 3, Apache Kafka, and Docker Compose.
Two independent services communicate exclusively through Kafka — no direct HTTP calls between them.

---

## Architecture

```
[HTTP Client]
     │
     ▼ POST /orders
[Order API :8080]
     │
     │  publishes OrderEvent (JSON)
     ▼
[Kafka: orders topic]
     │
     ▼  @KafkaListener (manual ack)
[Inventory Service :8081]
     ├── RESERVED   → logs success, commits offset
     ├── REJECTED   → logs warning (insufficient stock), commits offset
     └── UNKNOWN    → logs warning (unknown itemId), commits offset
          │
          └── on unexpected exception → [Kafka: orders.DLQ]
```

### Modules

| Module | Description |
|---|---|
| `shared-models` | `OrderEvent` POJO shared between both services |
| `order-api` | REST API — accepts orders, publishes to Kafka |
| `inventory-service` | Kafka consumer — processes orders against in-memory stock |

---

## Production-Grade Integration Mechanisms

| Mechanism | Where | Why |
|---|---|---|
| Idempotent producer (`enable.idempotence=true`) | Order API | Prevents duplicate messages on broker-side retries |
| `acks=all` | Order API | Message is not confirmed until all in-sync replicas have written it |
| `orderId` as partition key | Order API | Per-order event ordering guaranteed within a partition |
| Manual offset commit (`MANUAL_IMMEDIATE`) | Inventory Service | Offset committed only after successful processing — no message loss on crash |
| `DefaultErrorHandler` + `ExponentialBackOff` | Inventory Service | Retries failed messages 3 times (1s → 2s → 4s) before giving up |
| Dead Letter Topic (`orders.DLQ`) | Inventory Service | Exhausted messages parked for inspection without blocking the main topic |
| No Spring type headers on messages | Both | Messages are broker-agnostic; consumer does not depend on producer's framework |
| Docker health checks + `depends_on` | Docker Compose | Services start only after Kafka is ready |
| Non-root container user | Dockerfiles | Reduced attack surface |

---

## In-Memory Inventory

Initial stock (hardcoded, resets on service restart):

| Item | Stock |
|---|---|
| `item-1` | 10 |
| `item-2` | 5 |
| `item-3` | 0 |

---

## Prerequisites

- Docker and Docker Compose (v2)
- Java 21 and Maven 3.9+ (for running tests locally)

---

## Running Locally

### Start the full stack

```bash
docker compose up --build
```

All three containers start in the correct order:
1. **kafka** — waits until healthy (broker accepting connections)
2. **order-api** and **inventory-service** — start only after Kafka is healthy

To verify all services are up:

```bash
docker compose ps
```

Expected output — all services should show `(healthy)`:

```
NAME                    STATUS
mds-inventory-service   Up (healthy)
mds-kafka               Up (healthy)
mds-order-api           Up (healthy)
```

### Stop the stack

```bash
docker compose down
```

---

## Usage

### POST /orders — Place an order

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-001", "itemId": "item-1", "quantity": 2}'
```

**Response — 202 Accepted:**
```json
{
  "orderId": "order-001",
  "status": "ACCEPTED",
  "message": "Order event published successfully"
}
```

**Response — 400 Bad Request (validation failure):**
```json
{
  "status": 400,
  "error": "Validation Failed",
  "messages": ["quantity: quantity must be a positive number"]
}
```

### Test scenarios

**Successful reservation** (`item-1` has 10 units):
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-002", "itemId": "item-1", "quantity": 3}'
```

Inventory Service log:
```
INVENTORY_RESERVED: orderId=order-002, itemId=item-1, quantity=3, message=Reserved 3 unit(s) of 'item-1' for order 'order-002'
```

**Rejected — insufficient stock** (`item-3` has 0 units):
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-003", "itemId": "item-3", "quantity": 1}'
```

Inventory Service log:
```
INVENTORY_REJECTED: orderId=order-003, itemId=item-3, requested=1, available=0
```

**Rejected — unknown item:**
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-004", "itemId": "item-99", "quantity": 1}'
```

Inventory Service log:
```
INVENTORY_UNKNOWN_ITEM: orderId=order-004, itemId=item-99, message=Item 'item-99' not found in inventory catalog
```

### Watch live logs

```bash
# All services
docker compose logs -f

# Only inventory processing
docker compose logs -f inventory-service

# Only order publishing
docker compose logs -f order-api
```

### Health endpoints

```bash
curl http://localhost:8080/actuator/health   # Order API
curl http://localhost:8081/actuator/health   # Inventory Service
```

---

## Running Tests

All tests run without Docker — they use Spring Kafka's `EmbeddedKafka`.

```bash
# All modules
mvn test

# Specific module
mvn test -pl shared-models
mvn test -pl order-api
mvn test -pl inventory-service
```

Test summary:

| Module | Tests | Coverage |
|---|---|---|
| `shared-models` | 7 | OrderEvent serialization / validation |
| `order-api` | 12 | MockMvc controller (9) + EmbeddedKafka integration (3) |
| `inventory-service` | 13 | InventoryService unit (10) + EmbeddedKafka integration (3) |
| **Total** | **32** | |

---

## Project Structure

```
MDS/
├── docker-compose.yml
├── pom.xml                          # Parent — Java 21, Spring Boot 3.2.5 BOM
├── shared-models/                   # Shared OrderEvent model
│   └── src/main/java/com/mds/shared/model/OrderEvent.java
├── order-api/                       # REST API + Kafka producer
│   └── src/main/java/com/mds/orderapi/
│       ├── controller/OrderController.java
│       ├── service/KafkaOrderPublisherService.java
│       └── config/KafkaProducerConfig.java
└── inventory-service/               # Kafka consumer + in-memory inventory
    └── src/main/java/com/mds/inventory/
        ├── consumer/OrderEventConsumer.java
        ├── service/InventoryService.java
        └── config/KafkaConsumerConfig.java
```

---

## Key Technical Decisions

**Kafka over RabbitMQ** — Chosen for its durability model (`acks=all`), replayability (offset-based consumption), and native DLQ support via topic naming convention.

**KRaft mode (no Zookeeper)** — Kafka 3.7 runs in KRaft mode, eliminating the Zookeeper dependency and simplifying the Docker Compose setup from 3 infrastructure services to 1.

**`shared-models` module** — Ensures `OrderEvent` schema is defined once and shared. Producer and consumer always serialize/deserialize the same class, making schema drift a compile-time error rather than a runtime surprise.

**Business rejections are not exceptions** — Insufficient stock and unknown items are expected outcomes. They are logged as warnings and the offset is committed normally. Only unexpected runtime errors trigger the retry/DLQ path, preventing unnecessary redelivery of permanently invalid orders.

**`orderId` as partition key** — All events for the same order land on the same partition, guaranteeing that if an order ever produces multiple events, they are processed in order.
