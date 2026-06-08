package com.example.common.event;

import java.time.LocalDateTime;

public record EventEnvelope(
        String eventId,
        String eventType,
        String aggregateId,
        String aggregateType,
        LocalDateTime occurredAt,
        int schemaVersion,
        String correlationId,
        Object payload
) {
}
