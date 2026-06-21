package com.example.inventoryservice.outbox;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.InventoryReservedEvent;
import com.example.inventoryservice.entity.InventoryOutboxEvent;
import com.example.inventoryservice.repository.InventoryOutboxEventRepository;
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

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryOutboxRelayTest {

    @Mock
    private InventoryOutboxEventRepository outboxEventRepository;
    @Mock
    private KafkaOperations<String, Object> kafkaTemplate;
    @Mock
    private PlatformTransactionManager transactionManager;

    private InventoryOutboxRelay relay;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final Clock clock = Clock.fixed(Instant.parse("2026-09-24T12:00:00Z"), ZoneOffset.UTC);

    @BeforeEach
    void setUp() {
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
                .thenReturn(new SimpleTransactionStatus());
        relay = new InventoryOutboxRelay(outboxEventRepository, objectMapper, clock, transactionManager, kafkaTemplate);
        ReflectionTestUtils.setField(relay, "inventoryReplyTopic", "inventory-reply");
        ReflectionTestUtils.setField(relay, "maxAttempts", 5);
        ReflectionTestUtils.setField(relay, "initialBackoff", java.time.Duration.ofSeconds(2));
        ReflectionTestUtils.setField(relay, "maxBackoff", java.time.Duration.ofMinutes(1));
    }

    @Test
    void publishesStoredEnvelopeWithOrderIdKeyAndMarksSentByClaimToken() throws Exception {
        InventoryOutboxEvent outboxEvent = claimedEvent(validEnvelopeJson(), 1, "token-1");
        when(kafkaTemplate.send(eq("inventory-reply"), eq("order-1"), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxEventRepository.markSent(eq("reply-event-1"), eq("token-1"), any(LocalDateTime.class)))
                .thenReturn(1);

        relay.publishClaimedEvent(outboxEvent);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq("inventory-reply"), eq("order-1"), captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo("reply-event-1");
        assertThat(captor.getValue().correlationId()).isEqualTo("order-1");
        verify(outboxEventRepository).markSent(eq("reply-event-1"), eq("token-1"), any(LocalDateTime.class));
    }

    @Test
    void failedFutureSchedulesRetryWithoutChangingStoredPayloadOrEventId() throws Exception {
        InventoryOutboxEvent outboxEvent = claimedEvent(validEnvelopeJson(), 2, "token-1");
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("inventory-reply"), eq("order-1"), any(EventEnvelope.class))).thenReturn(future);

        relay.publishClaimedEvent(outboxEvent);
        future.completeExceptionally(new RuntimeException("broker unavailable"));

        assertThat(outboxEvent.getPayload()).contains("\"eventId\":\"reply-event-1\"");
        verify(outboxEventRepository).markPublishFailure(
                eq("reply-event-1"),
                eq("token-1"),
                eq("PENDING"),
                eq(LocalDateTime.of(2026, 9, 24, 12, 0, 4)),
                contains("broker unavailable")
        );
    }

    @Test
    void oldCallbackCannotOverwriteNewClaimToken() {
        InventoryOutboxEvent oldClaim = claimedEvent("{not-json", 1, "old-token");

        relay.publishClaimedEvent(oldClaim);

        verify(outboxEventRepository).markPublishFailure(
                eq("reply-event-1"),
                eq("old-token"),
                eq("FAILED"),
                eq(LocalDateTime.of(2026, 9, 24, 12, 0)),
                contains("Invalid inventory outbox payload")
        );
    }

    private InventoryOutboxEvent claimedEvent(String payload, int attemptCount, String claimToken) {
        return InventoryOutboxEvent.builder()
                .id("reply-event-1")
                .aggregateId("order-1")
                .aggregateType("INVENTORY")
                .eventType(EventTypes.INVENTORY_RESERVED)
                .payload(payload)
                .status("IN_PROGRESS")
                .createdAt(LocalDateTime.of(2026, 9, 24, 11, 59))
                .attemptCount(attemptCount)
                .nextAttemptAt(LocalDateTime.of(2026, 9, 24, 11, 59))
                .claimToken(claimToken)
                .claimedBy("test")
                .claimedUntil(LocalDateTime.of(2026, 9, 24, 12, 2))
                .build();
    }

    private String validEnvelopeJson() throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                "reply-event-1",
                EventTypes.INVENTORY_RESERVED,
                "order-1",
                "INVENTORY",
                LocalDateTime.of(2026, 9, 24, 12, 0),
                1,
                "order-1",
                new InventoryReservedEvent("order-1", "product-1", 2,
                        "Inventory reserved for: product-1")
        );
        return objectMapper.writeValueAsString(envelope);
    }
}
