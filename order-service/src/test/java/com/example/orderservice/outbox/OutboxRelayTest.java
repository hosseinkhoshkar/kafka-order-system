package com.example.orderservice.outbox;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.orderservice.entity.OutboxEvent;
import com.example.orderservice.observability.OrderObservabilityMetrics;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaOperations<String, Object> kafkaTemplate;

    @Mock
    private PlatformTransactionManager transactionManager;
    @Mock
    private OrderObservabilityMetrics metrics;

    private OutboxRelay outboxRelay;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final Clock clock = Clock.fixed(Instant.parse("2026-09-24T12:00:00Z"), ZoneOffset.UTC);

    @BeforeEach
    void setUp() {
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
                .thenReturn(new SimpleTransactionStatus());
        outboxRelay = new OutboxRelay(outboxEventRepository, objectMapper, clock, transactionManager, kafkaTemplate, metrics);
        ReflectionTestUtils.setField(outboxRelay, "ordersTopic", "orders");
        ReflectionTestUtils.setField(outboxRelay, "maxAttempts", 5);
        ReflectionTestUtils.setField(outboxRelay, "initialBackoff", java.time.Duration.ofSeconds(2));
        ReflectionTestUtils.setField(outboxRelay, "maxBackoff", java.time.Duration.ofMinutes(1));
    }

    @Test
    void publishesEnvelopeFromOutboxPayloadAndMarksSentByToken() throws Exception {
        OutboxEvent outboxEvent = claimedEvent(validEnvelopeJson(), 1);
        when(kafkaTemplate.send(eq("orders"), eq("order-1"), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxEventRepository.markSent(eq("outbox-1"), eq("token-1"), any(LocalDateTime.class)))
                .thenReturn(1);

        outboxRelay.publishClaimedEvent(outboxEvent);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq("orders"), eq("order-1"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(captor.getValue().aggregateId()).isEqualTo("order-1");
        verify(outboxEventRepository).markSent(eq("outbox-1"), eq("token-1"), any(LocalDateTime.class));
    }

    @Test
    void failedFutureSchedulesRetryWithoutChangingPayload() throws Exception {
        OutboxEvent outboxEvent = claimedEvent(validEnvelopeJson(), 2);

        CompletableFuture<SendResult<String, Object>> future =
                new CompletableFuture<>();

        when(kafkaTemplate.send(
                eq("orders"),
                eq("order-1"),
                any(EventEnvelope.class)
        )).thenReturn(future);

        outboxRelay.publishClaimedEvent(outboxEvent);

        future.completeExceptionally(
                new RuntimeException("broker unavailable")
        );

        verify(outboxEventRepository).markPublishFailure(
                eq("outbox-1"),
                eq("token-1"),
                eq("PENDING"),
                eq(LocalDateTime.of(2026, 9, 24, 12, 0, 4)),
                contains("broker unavailable")
        );
    }

    @Test
    void invalidPayloadBecomesFinalFailure() {
        OutboxEvent outboxEvent = claimedEvent("{not-json", 1);

        outboxRelay.publishClaimedEvent(outboxEvent);

        verify(outboxEventRepository).markPublishFailure(
                eq("outbox-1"),
                eq("token-1"),
                eq("FAILED"),
                eq(LocalDateTime.of(2026, 9, 24, 12, 0)),
                contains("Invalid outbox payload")
        );
    }

    private OutboxEvent claimedEvent(String payload, int attemptCount) {
        return OutboxEvent.builder()
                .id("outbox-1")
                .aggregateId("order-1")
                .aggregateType("ORDER")
                .eventType(EventTypes.ORDER_CREATED)
                .payload(payload)
                .status("IN_PROGRESS")
                .createdAt(LocalDateTime.of(2026, 9, 24, 11, 59))
                .attemptCount(attemptCount)
                .nextAttemptAt(LocalDateTime.of(2026, 9, 24, 11, 59))
                .claimToken("token-1")
                .claimedBy("test")
                .claimedUntil(LocalDateTime.of(2026, 9, 24, 12, 2))
                .build();
    }

    private String validEnvelopeJson() throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                "event-1",
                EventTypes.ORDER_CREATED,
                "order-1",
                "ORDER",
                LocalDateTime.of(2026, 9, 24, 12, 0),
                1,
                "order-1",
                new OrderCreatedEvent(
                        "order-1",
                        "product-1",
                        "customer-1",
                        2,
                        BigDecimal.TEN,
                        "PENDING",
                        LocalDateTime.of(2026, 9, 24, 12, 0)
                )
        );
        return objectMapper.writeValueAsString(envelope);
    }
}
