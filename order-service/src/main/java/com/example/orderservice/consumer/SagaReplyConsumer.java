package com.example.orderservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.orderservice.observability.OrderObservabilityMetrics;
import com.example.orderservice.service.InventoryReplyProcessingResult;
import com.example.orderservice.service.InventoryReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaReplyConsumer {

    private final InventoryReplyService inventoryReplyService;
    private final OrderObservabilityMetrics metrics;

    @KafkaListener(topics = "${kafka.topic.inventory-reply}", groupId = "${kafka.consumer.group.order}")
    public void consumeInventoryReply(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope reply = record.value();
        try (MDC.MDCCloseable orderId = MDC.putCloseable("orderId", valueOrEmpty(reply == null ? record.key() : reply.aggregateId()));
             MDC.MDCCloseable eventId = MDC.putCloseable("eventId", valueOrEmpty(reply == null ? null : reply.eventId()));
             MDC.MDCCloseable correlationId = MDC.putCloseable("correlationId", valueOrEmpty(reply == null ? null : reply.correlationId()))) {
            InventoryReplyProcessingResult result = inventoryReplyService.process(reply);
            log.info("Inventory reply processed | outcome: {} | partition: {} | offset: {}",
                    result.outcome(), record.partition(), record.offset());
        }
    }

    @KafkaListener(
            topics = "${kafka.topic.inventory-reply-dlt}",
            groupId = "${kafka.consumer.group.order-dlt}",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        metrics.dltObserved(record.topic());
        log.error("Inventory reply DLT observed | key: {} | topic: {} | partition: {} | offset: {} | bytes: {}",
                record.key(), record.topic(), record.partition(), record.offset(),
                record.value() == null ? 0 : record.value().length);
    }

    private String valueOrEmpty(String value) {
        return value == null ? "" : value;
    }
}
