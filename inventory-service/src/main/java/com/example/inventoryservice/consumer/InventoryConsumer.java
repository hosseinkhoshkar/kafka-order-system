package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.inventoryservice.observability.InventoryObservabilityMetrics;
import com.example.inventoryservice.service.InventoryProcessingResult;
import com.example.inventoryservice.service.InventoryReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryConsumer {

    private final InventoryReservationService inventoryReservationService;
    private final InventoryObservabilityMetrics metrics;

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "${kafka.consumer.group.inventory}")
    public void consumeOrder(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        try (MDC.MDCCloseable orderId = MDC.putCloseable("orderId", valueOrEmpty(envelope == null ? record.key() : envelope.aggregateId()));
             MDC.MDCCloseable eventId = MDC.putCloseable("eventId", valueOrEmpty(envelope == null ? null : envelope.eventId()));
             MDC.MDCCloseable correlationId = MDC.putCloseable("correlationId", valueOrEmpty(envelope == null ? null : envelope.correlationId()))) {
            log.info("Order event received | partition: {} | offset: {}", record.partition(), record.offset());
            InventoryProcessingResult result = inventoryReservationService.process(envelope);
            log.info("Inventory event processed | outcome: {}", result.outcome());
        }
    }

    @KafkaListener(
            topics = "${kafka.topic.orders-dlt}",
            groupId = "${kafka.consumer.group.inventory-dlt}",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        metrics.dltObserved(record.topic());
        log.error("Order DLT observed | key: {} | topic: {} | partition: {} | offset: {} | bytes: {}",
                record.key(), record.topic(), record.partition(), record.offset(),
                record.value() == null ? 0 : record.value().length);
    }

    private String valueOrEmpty(String value) {
        return value == null ? "" : value;
    }
}
