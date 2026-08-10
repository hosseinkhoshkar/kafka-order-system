package com.example.orderservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.orderservice.service.InventoryReplyProcessingResult;
import com.example.orderservice.service.InventoryReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaReplyConsumer {

    private final InventoryReplyService inventoryReplyService;

    @KafkaListener(topics = "${kafka.topic.inventory-reply}", groupId = "${kafka.consumer.group.order}")
    public void consumeInventoryReply(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope reply = record.value();
        InventoryReplyProcessingResult result = inventoryReplyService.process(reply);
        log.info("Inventory reply processed | orderId: {} | outcome: {} | partition: {} | offset: {}",
                result.orderId(), result.outcome(), record.partition(), record.offset());
    }

    @KafkaListener(
            topics = "${kafka.topic.inventory-reply-dlt}",
            groupId = "${kafka.consumer.group.order-dlt}",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        log.error("Inventory reply DLT observed | key: {} | topic: {} | partition: {} | offset: {} | bytes: {}",
                record.key(), record.topic(), record.partition(), record.offset(),
                record.value() == null ? 0 : record.value().length);
    }
}
