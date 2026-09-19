package com.example.inventoryservice.observability;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@RequiredArgsConstructor
public class InventoryObservabilityMetrics {

    private final MeterRegistry registry;

    public void reservationResultAfterCommit(String result) {
        afterCommit(() -> registry.counter("inventory_reservations_total",
                "result", result).increment());
    }

    public void inboxDuplicateAfterCommit(String eventType) {
        afterCommit(() -> registry.counter("inventory_inbox_duplicates_total",
                "event_type", eventType).increment());
    }

    public void outboxPublishAttempt(String eventType) {
        registry.counter("inventory_outbox_publish_attempts_total", "event_type", eventType).increment();
    }

    public void outboxPublishSuccess(String eventType) {
        registry.counter("inventory_outbox_publish_success_total", "event_type", eventType).increment();
    }

    public void outboxPublishFailure(String eventType, String outcome) {
        registry.counter("inventory_outbox_publish_failures_total",
                "event_type", eventType,
                "outcome", outcome).increment();
    }

    public void dltProducerResult(String topic, String result) {
        registry.counter("inventory_kafka_dlt_publish_total",
                "topic", topic,
                "result", result).increment();
    }

    public void dltObserved(String topic) {
        registry.counter("inventory_kafka_dlt_observed_total", "topic", topic).increment();
    }

    private void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}
