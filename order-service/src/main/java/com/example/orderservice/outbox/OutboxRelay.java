package com.example.orderservice.outbox;

import com.example.common.event.EventEnvelope;
import com.example.orderservice.entity.OutboxEvent;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    @Qualifier("objectKafkaTemplate")
    private final KafkaOperations<String, Object> kafkaTemplate;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatus("PENDING");

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Processing {} outbox events", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                EventEnvelope envelope = objectMapper.readValue(event.getPayload(), EventEnvelope.class);
                kafkaTemplate.send(ordersTopic, event.getAggregateId(), envelope)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                event.setStatus("SENT");
                                event.setSentAt(LocalDateTime.now());
                                outboxEventRepository.save(event);
                                log.info("Outbox event sent | id: {} | type: {}",
                                        event.getId(), event.getEventType());
                            } else {
                                event.setStatus("FAILED");
                                outboxEventRepository.save(event);
                                log.error("Outbox event failed | id: {} | error: {}",
                                        event.getId(), ex.getMessage());
                            }
                        });
            } catch (JsonProcessingException ex) {
                event.setStatus("FAILED");
                outboxEventRepository.save(event);
                log.error("Invalid outbox payload | id: {} | error: {}", event.getId(), ex.getMessage());
            }
        }
    }
}
