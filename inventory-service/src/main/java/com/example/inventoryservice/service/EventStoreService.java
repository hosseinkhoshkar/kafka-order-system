package com.example.inventoryservice.service;

import com.example.inventoryservice.entity.EventStore;
import com.example.inventoryservice.repository.EventStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventStoreService {

    private final EventStoreRepository eventStoreRepository;
    private final ObjectMapper objectMapper;

    public void saveEvent(String aggregateId, String aggregateType,
                          String eventType, Object payload) {
        try {
            int nextVersion = Math.toIntExact(eventStoreRepository.countByAggregateId(aggregateId) + 1);
            EventStore event = EventStore.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(aggregateId)
                    .aggregateType(aggregateType)
                    .eventType(eventType)
                    .payload(objectMapper.writeValueAsString(payload))
                    .version(nextVersion)
                    .occurredAt(LocalDateTime.now())
                    .build();

            eventStoreRepository.save(event);
            log.info("Event saved | aggregateId: {} | type: {} | version: {}",
                    aggregateId, eventType, nextVersion);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize inventory event | aggregateId: {} | error: {}",
                    aggregateId, e.getMessage());
            throw new IllegalStateException("Failed to save inventory event for aggregate " + aggregateId, e);
        }
    }

    public List<EventStore> getEvents(String aggregateId) {
        return eventStoreRepository.findByAggregateIdOrderByVersionAsc(aggregateId);
    }
}
