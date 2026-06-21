package com.example.inventoryservice.service;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.inventoryservice.entity.InventoryEntity;
import com.example.inventoryservice.entity.InventoryOutboxEvent;
import com.example.inventoryservice.exception.InvalidInventoryMessageException;
import com.example.inventoryservice.exception.OrderEventConflictException;
import com.example.inventoryservice.repository.EventStoreRepository;
import com.example.inventoryservice.repository.InventoryInboxEventRepository;
import com.example.inventoryservice.repository.InventoryOutboxEventRepository;
import com.example.inventoryservice.repository.InventoryRepository;
import com.example.inventoryservice.repository.InventoryReservationDecisionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.jpa.show-sql=false",
        "spring.kafka.bootstrap-servers=localhost:65535",
        "spring.kafka.listener.auto-startup=false",
        "inventory.outbox.relay.poll-interval-ms=600000"
})
class InventoryReservationServiceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15.6")
            .withDatabaseName("inventorydb");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    InventoryReservationService reservationService;
    @Autowired
    InventoryRepository inventoryRepository;
    @Autowired
    InventoryInboxEventRepository inboxRepository;
    @Autowired
    InventoryReservationDecisionRepository decisionRepository;
    @Autowired
    InventoryOutboxEventRepository outboxRepository;
    @Autowired
    EventStoreRepository eventStoreRepository;
    @Autowired
    EntityManager entityManager;

    @BeforeEach
    void cleanDatabase() {
        eventStoreRepository.deleteAll();
        outboxRepository.deleteAll();
        decisionRepository.deleteAll();
        inboxRepository.deleteAll();
        inventoryRepository.deleteAll();
        FailableEventStoreService.fail.set(false);
    }

    @Test
    void reservesAvailableInventoryAndPersistsDecisionHistoryAndOutboxAtomically() {
        inventory("product-1", 20, 0);

        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 11));

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(9);
        assertThat(inventory.getReservedQuantity()).isEqualTo(11);
        assertThat(decisionRepository.findById("order-1").orElseThrow().getStatus()).isEqualTo("RESERVED");
        assertThat(eventStoreRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.findAll().get(0).getEventType()).isEqualTo(EventTypes.INVENTORY_RESERVED);
    }

    @Test
    void insufficientAndMissingInventoryCreateBusinessFailureWithoutChangingStock() {
        inventory("product-1", 3, 0);

        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 4));
        reservationService.process(orderEnvelope("event-2", "order-2", "missing", 1));

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(3);
        assertThat(inventory.getReservedQuantity()).isZero();
        assertThat(decisionRepository.findAll()).extracting("status").containsOnly("FAILED");
        assertThat(outboxRepository.findAll()).extracting("eventType")
                .containsOnly(EventTypes.INVENTORY_RESERVATION_FAILED);
    }

    @Test
    void repeatedSameEventOnlyCreatesOneDecisionAndOutboxEvent() {
        inventory("product-1", 20, 0);
        EventEnvelope envelope = orderEnvelope("event-1", "order-1", "product-1", 2);

        IntStream.range(0, 10).forEach(index -> reservationService.process(envelope));

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(18);
        assertThat(inventory.getReservedQuantity()).isEqualTo(2);
        assertThat(inboxRepository.count()).isEqualTo(1);
        assertThat(decisionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void sameEventIdWithDifferentContentIsInvalidAndDoesNotCreateSecondEffect() {
        inventory("product-1", 20, 0);
        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 2));

        assertThatThrownBy(() -> reservationService.process(orderEnvelope("event-1", "order-2", "product-1", 2)))
                .isInstanceOf(InvalidInventoryMessageException.class);

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(18);
        assertThat(inventory.getReservedQuantity()).isEqualTo(2);
        assertThat(inboxRepository.count()).isEqualTo(1);
        assertThat(decisionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void concurrentDuplicateEventIsIdempotent() throws Exception {
        inventory("product-1", 20, 0);
        EventEnvelope envelope = orderEnvelope("event-1", "order-1", "product-1", 2);
        runConcurrently(10, () -> {
            reservationService.process(envelope);
            return null;
        });

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(18);
        assertThat(inventory.getReservedQuantity()).isEqualTo(2);
        assertThat(decisionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void sameOrderWithNewEventAndSameDetailsKeepsExistingDecision() {
        inventory("product-1", 20, 0);

        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 2));
        reservationService.process(orderEnvelope("event-2", "order-1", "product-1", 2));

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(18);
        assertThat(decisionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
        assertThat(inboxRepository.count()).isEqualTo(2);
    }

    @Test
    void sameOrderWithDifferentProductOrQuantityIsConflictWithoutBusinessResponse() {
        inventory("product-1", 20, 0);
        inventory("product-2", 20, 0);
        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 2));

        assertThatThrownBy(() -> reservationService.process(orderEnvelope("event-2", "order-1", "product-2", 2)))
                .isInstanceOf(OrderEventConflictException.class);
        assertThatThrownBy(() -> reservationService.process(orderEnvelope("event-3", "order-1", "product-1", 3)))
                .isInstanceOf(OrderEventConflictException.class);

        assertThat(inventoryRepository.findById("product-1").orElseThrow().getAvailableQuantity()).isEqualTo(18);
        assertThat(inventoryRepository.findById("product-2").orElseThrow().getAvailableQuantity()).isEqualTo(20);
        assertThat(decisionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void failedDecisionDoesNotBecomeReservedAfterStockIncreaseAndRedelivery() {
        inventory("product-1", 1, 0);
        reservationService.process(orderEnvelope("event-1", "order-1", "product-1", 2));
        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        inventory.setAvailableQuantity(10);
        inventoryRepository.save(inventory);

        reservationService.process(orderEnvelope("event-2", "order-1", "product-1", 2));

        InventoryEntity after = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(after.getAvailableQuantity()).isEqualTo(10);
        assertThat(after.getReservedQuantity()).isZero();
        assertThat(decisionRepository.findById("order-1").orElseThrow().getStatus()).isEqualTo("FAILED");
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void concurrentOrdersCannotOversellInventory() throws Exception {
        inventory("product-1", 10, 0);

        runConcurrently(20, index -> {
            reservationService.process(orderEnvelope("event-" + index, "order-" + index, "product-1", 1));
            return null;
        });

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isZero();
        assertThat(inventory.getReservedQuantity()).isEqualTo(10);
        assertThat(decisionRepository.findAll()).filteredOn(decision -> "RESERVED".equals(decision.getStatus()))
                .hasSize(10);
        assertThat(decisionRepository.findAll()).filteredOn(decision -> "FAILED".equals(decision.getStatus()))
                .hasSize(10);
    }

    @Test
    void serializationFailureRollsBackInboxDecisionInventoryHistoryAndOutboxThenRetrySucceeds() throws Exception {
        inventory("product-1", 5, 0);
        FailableEventStoreService.fail.set(true);

        EventEnvelope envelope = orderEnvelope("event-1", "order-1", "product-1", 2);
        assertThatThrownBy(() -> reservationService.process(envelope))
                .isInstanceOf(IllegalStateException.class);

        InventoryEntity inventory = inventoryRepository.findById("product-1").orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(5);
        assertThat(inventory.getReservedQuantity()).isZero();
        assertThat(inboxRepository.count()).isZero();
        assertThat(decisionRepository.count()).isZero();
        assertThat(eventStoreRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();

        FailableEventStoreService.fail.set(false);
        reservationService.process(envelope);
        assertThat(inventoryRepository.findById("product-1").orElseThrow().getAvailableQuantity()).isEqualTo(3);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    @Transactional
    void expiredOutboxClaimCanBeRecoveredAndOldCallbackCannotOverwriteNewClaim() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 24, 12, 0);
        outboxRepository.save(InventoryOutboxEvent.builder()
                .id("reply-event-1")
                .aggregateId("order-1")
                .aggregateType("INVENTORY")
                .eventType(EventTypes.INVENTORY_RESERVED)
                .payload("{}")
                .status("IN_PROGRESS")
                .createdAt(now.minusMinutes(10))
                .attemptCount(1)
                .nextAttemptAt(now.minusMinutes(10))
                .claimToken("old-token")
                .claimedBy("old-worker")
                .claimedUntil(now.minusMinutes(1))
                .build());
        entityManager.flush();
        entityManager.clear();

        List<InventoryOutboxEvent> claimed = outboxRepository.claimAvailable(
                now, now.plusMinutes(2), "new-token", "new-worker", 10, 5);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getClaimToken()).isEqualTo("new-token");
        assertThat(outboxRepository.markSent("reply-event-1", "old-token", now)).isZero();
        assertThat(outboxRepository.markSent("reply-event-1", "new-token", now)).isEqualTo(1);
        entityManager.flush();
        entityManager.clear();
        assertThat(outboxRepository.findById("reply-event-1").orElseThrow().getStatus()).isEqualTo("SENT");
    }

    private void inventory(String productId, int available, int reserved) {
        inventoryRepository.save(InventoryEntity.builder()
                .productId(productId)
                .availableQuantity(available)
                .reservedQuantity(reserved)
                .build());
    }

    private EventEnvelope orderEnvelope(String eventId, String orderId, String productId, int quantity) {
        LocalDateTime now = LocalDateTime.of(2026, 9, 24, 12, 0);
        return new EventEnvelope(
                eventId,
                EventTypes.ORDER_CREATED,
                orderId,
                "ORDER",
                now,
                1,
                orderId,
                new OrderCreatedEvent(orderId, productId, "customer-1", quantity,
                        BigDecimal.TEN, "PENDING", now)
        );
    }

    private void runConcurrently(int count, ThrowingIndexedCallable task) throws Exception {
        var executor = Executors.newFixedThreadPool(count);
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int index = i;
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                return task.call(index);
            }));
        }
        ready.await();
        start.countDown();
        for (Future<Void> future : futures) {
            future.get();
        }
        executor.shutdownNow();
    }

    private void runConcurrently(int count, Callable<Void> task) throws Exception {
        runConcurrently(count, ignored -> task.call());
    }

    @FunctionalInterface
    private interface ThrowingIndexedCallable {
        Void call(int index) throws Exception;
    }

    @TestConfiguration
    static class FailureInjectionConfig {
        @Bean
        @Primary
        EventStoreService eventStoreService(EventStoreRepository eventStoreRepository, ObjectMapper objectMapper) {
            return new FailableEventStoreService(eventStoreRepository, objectMapper);
        }
    }

    private static class FailableEventStoreService extends EventStoreService {
        private static final AtomicBoolean fail = new AtomicBoolean(false);

        FailableEventStoreService(EventStoreRepository eventStoreRepository, ObjectMapper objectMapper) {
            super(eventStoreRepository, objectMapper);
        }

        @Override
        public void saveEvent(String aggregateId, String aggregateType, String eventType, Object payload) {
            if (fail.get()) {
                throw new IllegalStateException("Injected event-store failure");
            }
            super.saveEvent(aggregateId, aggregateType, eventType, payload);
        }
    }
}
