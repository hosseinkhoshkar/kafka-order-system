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

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "${kafka.consumer.group.inventory}")
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

    @KafkaListener(
            topics = "${kafka.topic.orders-dlt}",
            groupId = "${kafka.consumer.group.inventory-dlt}",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        log.error("Order DLT observed | key: {} | topic: {} | partition: {} | offset: {} | bytes: {}",
                record.key(), record.topic(), record.partition(), record.offset(),
                record.value() == null ? 0 : record.value().length);
    }
}
