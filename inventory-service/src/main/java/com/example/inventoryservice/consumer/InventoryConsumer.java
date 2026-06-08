package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.InventoryReservationFailedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.example.common.event.OrderCreatedEvent;
import com.example.inventoryservice.service.EventStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryConsumer {

    private final KafkaOperations<String, Object> kafkaTemplate;
    private final EventStoreService eventStoreService;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.inventory-reply}")
    private String inventoryReplyTopic;

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "inventory-group")
    public void consumeOrder(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        if (!EventTypes.ORDER_CREATED.equals(envelope.eventType())) {
            log.warn("Ignoring unsupported event type: {}", envelope.eventType());
            return;
        }

        OrderCreatedEvent order = objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class);

        log.info("Order event received | id: {} | product: {} | qty: {} | partition: {} | offset: {}",
                order.orderId(),
                order.productId(),
                order.quantity(),
                record.partition(),
                record.offset()
        );

        EventEnvelope reply = processInventory(order, envelope.correlationId());

        eventStoreService.saveEvent(
                order.orderId(),
                "INVENTORY",
                reply.eventType(),
                reply
        );

        kafkaTemplate.send(inventoryReplyTopic, order.orderId(), reply);
        log.info("Reply sent | orderId: {} | eventType: {}", order.orderId(), reply.eventType());
    }

    @KafkaListener(topics = "orders.DLT", groupId = "inventory-group-dlt")
    public void consumeDeadLetter(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        log.error("Dead Letter received | aggregateId: {} | eventType: {}",
                envelope.aggregateId(),
                envelope.eventType()
        );
    }

    private EventEnvelope processInventory(OrderCreatedEvent order, String correlationId) {
        log.info("Processing inventory for product: {} | quantity: {}",
                order.productId(),
                order.quantity()
        );

        if (order.quantity() > 10) {
            log.warn("Insufficient inventory for order: {}", order.orderId());
            InventoryReservationFailedEvent payload = new InventoryReservationFailedEvent(
                    order.orderId(),
                    order.productId(),
                    order.quantity(),
                    "Insufficient inventory for: " + order.productId()
            );
            return createReplyEnvelope(EventTypes.INVENTORY_RESERVATION_FAILED, order.orderId(), correlationId, payload);
        }

        log.info("Inventory OK for order: {}", order.orderId());
        InventoryReservedEvent payload = new InventoryReservedEvent(
                order.orderId(),
                order.productId(),
                order.quantity(),
                "Inventory reserved for: " + order.productId()
        );
        return createReplyEnvelope(EventTypes.INVENTORY_RESERVED, order.orderId(), correlationId, payload);
    }

    private EventEnvelope createReplyEnvelope(String eventType, String orderId, String correlationId, Object payload) {
        return new EventEnvelope(
                UUID.randomUUID().toString(),
                eventType,
                orderId,
                "INVENTORY",
                LocalDateTime.now(),
                1,
                correlationId,
                payload
        );
    }
}
