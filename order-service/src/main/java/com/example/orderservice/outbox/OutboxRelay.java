package com.example.orderservice.outbox;

import com.example.orderservice.entity.OutboxEvent;
import com.example.orderservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 5000) // هر ۵ ثانیه
    @Transactional
    public void processOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatus("PENDING");

        if (pendingEvents.isEmpty()) return;

        log.info("🔄 Processing {} outbox events", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            kafkaTemplate.send("orders", event.getAggregateId(), event.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            event.setStatus("SENT");
                            event.setSentAt(LocalDateTime.now());
                            outboxEventRepository.save(event);
                            log.info("✅ Outbox event sent | id: {} | type: {}",
                                    event.getId(), event.getEventType());
                        } else {
                            event.setStatus("FAILED");
                            outboxEventRepository.save(event);
                            log.error("❌ Outbox event failed | id: {} | error: {}",
                                    event.getId(), ex.getMessage());
                        }
                    });
        }
    }
}