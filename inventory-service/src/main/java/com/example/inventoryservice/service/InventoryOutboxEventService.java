package com.example.inventoryservice.service;

import com.example.inventoryservice.entity.InventoryOutboxEvent;
import com.example.inventoryservice.repository.InventoryOutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class InventoryOutboxEventService {

    private final InventoryOutboxEventRepository inventoryOutboxEventRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public void saveOutboxEvent(String eventId, String aggregateId, String aggregateType,
                                String eventType, Object payload) {
        try {
            LocalDateTime now = LocalDateTime.now(clock);
            InventoryOutboxEvent outboxEvent = InventoryOutboxEvent.builder()
                    .id(eventId)
                    .aggregateId(aggregateId)
                    .aggregateType(aggregateType)
                    .eventType(eventType)
                    .payload(objectMapper.writeValueAsString(payload))
                    .status("PENDING")
                    .createdAt(now)
                    .attemptCount(0)
                    .nextAttemptAt(now)
                    .build();
            inventoryOutboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize inventory outbox event " + eventId, ex);
        }
    }
}
