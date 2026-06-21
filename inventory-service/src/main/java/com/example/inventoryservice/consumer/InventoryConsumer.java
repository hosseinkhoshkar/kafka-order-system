package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.inventoryservice.service.InventoryProcessingResult;
import com.example.inventoryservice.service.InventoryReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryConsumer {

    private final InventoryReservationService inventoryReservationService;

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "inventory-group")
    public void consumeOrder(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        log.info("Order event received | eventId: {} | aggregateId: {} | partition: {} | offset: {}",
                envelope == null ? null : envelope.eventId(),
                envelope == null ? null : envelope.aggregateId(),
                record.partition(),
                record.offset()
        );

        InventoryProcessingResult result = inventoryReservationService.process(envelope);
        log.info("Inventory event processed | orderId: {} | outcome: {}",
                result.orderId(), result.outcome());
    }

    @KafkaListener(topics = "orders.DLT", groupId = "inventory-group-dlt")
    public void consumeDeadLetter(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        log.error("Dead Letter received | aggregateId: {} | eventType: {}",
                envelope == null ? null : envelope.aggregateId(),
                envelope == null ? null : envelope.eventType()
        );
    }
}
