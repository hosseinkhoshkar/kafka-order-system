package com.example.inventoryservice.outbox;

import com.example.common.event.EventEnvelope;
import com.example.inventoryservice.entity.InventoryOutboxEvent;
import com.example.inventoryservice.observability.InventoryObservabilityMetrics;
import com.example.inventoryservice.repository.InventoryOutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryOutboxRelay {

    private static final int MAX_ERROR_LENGTH = 1024;

    private final InventoryOutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final PlatformTransactionManager transactionManager;
    private final KafkaOperations<String, Object> kafkaTemplate;
    private final InventoryObservabilityMetrics metrics;

    @Value("${kafka.topic.inventory-reply}")
    private String inventoryReplyTopic;
    @Value("${inventory.outbox.relay.batch-size:25}")
    private int batchSize;
    @Value("${inventory.outbox.relay.lease-duration:PT2M}")
    private Duration leaseDuration;
    @Value("${inventory.outbox.relay.initial-backoff:PT2S}")
    private Duration initialBackoff;
    @Value("${inventory.outbox.relay.max-backoff:PT1M}")
    private Duration maxBackoff;
    @Value("${inventory.outbox.relay.max-attempts:5}")
    private int maxAttempts;
    @Value("${spring.application.name:inventory-service}")
    private String applicationName;

    @Scheduled(fixedDelayString = "${inventory.outbox.relay.poll-interval-ms:5000}")
    public void processOutboxEvents() {
        markAttemptsExhausted();
        String claimToken = UUID.randomUUID().toString();
        List<InventoryOutboxEvent> claimedEvents = claimBatch(claimToken);
        if (claimedEvents.isEmpty()) {
            return;
        }

        log.info("Claimed {} inventory outbox events | token: {}", claimedEvents.size(), claimToken);
        for (InventoryOutboxEvent event : claimedEvents) {
            publishClaimedEvent(event);
        }
    }

    public List<InventoryOutboxEvent> claimBatch(String claimToken) {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        return transaction.execute(status -> {
            LocalDateTime now = now();
            return outboxEventRepository.claimAvailable(
                    now,
                    now.plus(leaseDuration),
                    claimToken,
                    applicationName,
                    batchSize,
                    maxAttempts
            );
        });
    }

    public void publishClaimedEvent(InventoryOutboxEvent event) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(event.getPayload(), EventEnvelope.class);
        } catch (JsonProcessingException ex) {
            recordFailure(event, "Invalid inventory outbox payload: " + ex.getOriginalMessage(), true);
            return;
        }

        CompletableFuture<?> sendFuture;
        try {
            metrics.outboxPublishAttempt(event.getEventType());
            sendFuture = kafkaTemplate.send(inventoryReplyTopic, event.getAggregateId(), envelope);
        } catch (Exception ex) {
            recordFailure(event, "Kafka send failed before acknowledgement: " + ex.getMessage(), false);
            return;
        }

        sendFuture.whenComplete((result, ex) -> {
            try (MDC.MDCCloseable orderId = MDC.putCloseable("orderId", event.getAggregateId());
                 MDC.MDCCloseable eventId = MDC.putCloseable("eventId", envelope.eventId());
                 MDC.MDCCloseable correlationId = MDC.putCloseable("correlationId", envelope.correlationId())) {
                if (ex == null) {
                    recordSuccess(event);
                } else {
                    recordFailure(event, "Kafka send failed: " + ex.getMessage(), false);
                }
            }
        });
    }

    public void recordSuccess(InventoryOutboxEvent event) {
        try {
            TransactionTemplate transaction = new TransactionTemplate(transactionManager);
            Integer updated = transaction.execute(status ->
                    outboxEventRepository.markSent(event.getId(), event.getClaimToken(), now()));
            if (updated == null || updated == 0) {
                log.warn("Inventory outbox SENT update skipped | id: {} | token: {}",
                        event.getId(), event.getClaimToken());
            } else {
                metrics.outboxPublishSuccess(event.getEventType());
            }
        } catch (Exception ex) {
            log.error("Failed to record inventory outbox send success | id: {} | error: {}",
                    event.getId(), ex.getMessage());
        }
    }

    public void recordFailure(InventoryOutboxEvent event, String error, boolean permanent) {
        try {
            TransactionTemplate transaction = new TransactionTemplate(transactionManager);
            transaction.executeWithoutResult(status -> {
                String nextStatus = permanent || event.getAttemptCount() >= maxAttempts ? "FAILED" : "PENDING";
                LocalDateTime nextAttemptAt = "FAILED".equals(nextStatus) ? now() : now().plus(backoff(event.getAttemptCount()));
                int updated = outboxEventRepository.markPublishFailure(
                        event.getId(),
                        event.getClaimToken(),
                        nextStatus,
                        nextAttemptAt,
                        sanitizeError(error)
                );
                if (updated == 0) {
                    log.warn("Inventory outbox failure update skipped | id: {} | token: {}",
                            event.getId(), event.getClaimToken());
                } else {
                    metrics.outboxPublishFailure(event.getEventType(), nextStatus);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to record inventory outbox send failure | id: {} | error: {}",
                    event.getId(), ex.getMessage());
        }
    }

    private void markAttemptsExhausted() {
        try {
            TransactionTemplate transaction = new TransactionTemplate(transactionManager);
            transaction.executeWithoutResult(status -> outboxEventRepository.markAttemptsExhausted(
                    maxAttempts,
                    "Inventory outbox retry attempts exhausted"
            ));
        } catch (Exception ex) {
            log.error("Failed to mark exhausted inventory outbox events | error: {}", ex.getMessage());
        }
    }

    private Duration backoff(int attemptCount) {
        long multiplier = 1L << Math.min(Math.max(attemptCount - 1, 0), 20);
        Duration backoff = initialBackoff.multipliedBy(multiplier);
        return backoff.compareTo(maxBackoff) > 0 ? maxBackoff : backoff;
    }

    private String sanitizeError(String error) {
        String sanitized = error == null ? "Unknown inventory outbox publish failure" : error.replaceAll("\\s+", " ").trim();
        return sanitized.length() <= MAX_ERROR_LENGTH ? sanitized : sanitized.substring(0, MAX_ERROR_LENGTH);
    }

    private LocalDateTime now() {
        return LocalDateTime.now(clock);
    }
}
