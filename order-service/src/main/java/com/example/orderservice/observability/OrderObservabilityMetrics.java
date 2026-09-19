package com.example.orderservice.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@RequiredArgsConstructor
public class OrderObservabilityMetrics {

    private final MeterRegistry registry;

    public void orderCreatedAfterCommit() {
        afterCommit(() -> registry.counter("order_orders_created_total").increment());
    }

    public void orderStatusTransitionAfterCommit(String from, String to) {
        afterCommit(() -> registry.counter("order_status_transitions_total",
                "from", from,
                "to", to).increment());
    }

    public void inboxDuplicateAfterCommit(String eventType) {
        afterCommit(() -> registry.counter("order_inbox_duplicates_total",
                "event_type", eventType).increment());
    }

    public void httpIdempotencyReplay() {
        registry.counter("order_http_idempotency_replays_total").increment();
    }

    public void httpIdempotencyConflict() {
        registry.counter("order_http_idempotency_conflicts_total").increment();
    }

    public void outboxPublishAttempt(String eventType) {
        registry.counter("order_outbox_publish_attempts_total", "event_type", eventType).increment();
    }

    public void outboxPublishSuccess(String eventType) {
        registry.counter("order_outbox_publish_success_total", "event_type", eventType).increment();
    }

    public void outboxPublishFailure(String eventType, String outcome) {
        registry.counter("order_outbox_publish_failures_total",
                "event_type", eventType,
                "outcome", outcome).increment();
    }

    public void dltProducerResult(String topic, String result) {
        registry.counter("order_kafka_dlt_publish_total",
                "topic", topic,
                "result", result).increment();
    }

    public void dltObserved(String topic) {
        registry.counter("order_kafka_dlt_observed_total", "topic", topic).increment();
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
