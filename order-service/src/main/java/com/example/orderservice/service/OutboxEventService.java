package com.example.orderservice.service;

import com.example.orderservice.entity.OutboxEvent;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public void saveOutboxEvent(String aggregateId, String aggregateType,
                                String eventType, Object payload) {
        try {
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(aggregateId)
                    .aggregateType(aggregateType)
                    .eventType(eventType)
                    .payload(objectMapper.writeValueAsString(payload))
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);
            log.info("📬 Outbox event saved | aggregateId: {} | type: {}",
                    aggregateId, eventType);

        } catch (JsonProcessingException e) {
            log.error("❌ Failed to save outbox event | aggregateId: {} | error: {}",
                    aggregateId, e.getMessage());
        }
    }
}