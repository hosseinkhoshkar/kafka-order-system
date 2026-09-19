package com.example.orderservice.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component("orderOutbox")
@RequiredArgsConstructor
public class OrderOutboxObservability implements HealthIndicator {

    private static final String[] STATUSES = {"PENDING", "IN_PROGRESS", "FAILED", "SENT"};

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final AtomicLong oldestUnpublishedAgeSeconds = new AtomicLong(0);
    private final AtomicLong lastRefreshSuccessful = new AtomicLong(1);

    @PostConstruct
    void registerMeters() {
        for (String status : STATUSES) {
            AtomicLong value = counts.computeIfAbsent(status, ignored -> new AtomicLong(0));
            Gauge.builder("order_outbox_events", value, AtomicLong::get)
                    .description("Cached number of order outbox rows by status.")
                    .tag("status", status)
                    .register(registry);
        }
        Gauge.builder("order_outbox_oldest_unpublished_age_seconds",
                        oldestUnpublishedAgeSeconds, AtomicLong::get)
                .description("Cached age in seconds of the oldest non-SENT order outbox row, or 0 when none exists.")
                .register(registry);
        refresh();
    }

    @Scheduled(fixedDelayString = "${observability.outbox.metrics-refresh-ms:10000}")
    public void refresh() {
        try {
            for (String status : STATUSES) {
                Long count = jdbcTemplate.queryForObject(
                        "select count(*) from outbox_events where status = ?",
                        Long.class,
                        status);
                counts.computeIfAbsent(status, ignored -> new AtomicLong()).set(count == null ? 0 : count);
            }
            Long oldestAge = jdbcTemplate.queryForObject("""
                    select coalesce(cast(extract(epoch from (now() - min(created_at))) as bigint), 0)
                    from outbox_events
                    where status <> 'SENT'
                    """, Long.class);
            oldestUnpublishedAgeSeconds.set(oldestAge == null ? 0 : oldestAge);
            lastRefreshSuccessful.set(1);
        } catch (Exception ex) {
            lastRefreshSuccessful.set(0);
            log.warn("Order outbox metrics refresh failed: {}", ex.getMessage());
        }
    }

    @Override
    public Health health() {
        Health.Builder builder = lastRefreshSuccessful.get() == 1 ? Health.up() : Health.unknown();
        builder.withDetail("pending", counts.getOrDefault("PENDING", new AtomicLong()).get());
        builder.withDetail("inProgress", counts.getOrDefault("IN_PROGRESS", new AtomicLong()).get());
        builder.withDetail("failed", counts.getOrDefault("FAILED", new AtomicLong()).get());
        builder.withDetail("oldestUnpublishedAgeSeconds", oldestUnpublishedAgeSeconds.get());
        return builder.build();
    }
}
