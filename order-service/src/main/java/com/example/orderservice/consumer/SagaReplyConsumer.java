package com.example.orderservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.InventoryReservationFailedEvent;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.EventStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaReplyConsumer {

    private final OrderRepository orderRepository;
    private final EventStoreService eventStoreService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${kafka.topic.inventory-reply}", groupId = "order-group")
    public void consumeInventoryReply(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope reply = record.value();

        log.info("Inventory reply received | orderId: {} | eventType: {}",
                reply.aggregateId(), reply.eventType());

        Optional<OrderEntity> orderOpt = orderRepository.findById(reply.aggregateId());

        if (orderOpt.isEmpty()) {
            log.error("Order not found in DB | orderId: {}", reply.aggregateId());
            return;
        }

        OrderEntity order = orderOpt.get();

        if (EventTypes.INVENTORY_RESERVED.equals(reply.eventType())) {
            order.setStatus(OrderStatus.CONFIRMED);
            eventStoreService.saveEvent(order.getOrderId(), "ORDER", "ORDER_CONFIRMED", reply);
            log.info("Order CONFIRMED | orderId: {}", reply.aggregateId());
        } else if (EventTypes.INVENTORY_RESERVATION_FAILED.equals(reply.eventType())) {
            order.setStatus(OrderStatus.CANCELLED);
            eventStoreService.saveEvent(order.getOrderId(), "ORDER", "ORDER_CANCELLED", reply);
            InventoryReservationFailedEvent failedEvent =
                    objectMapper.convertValue(reply.payload(), InventoryReservationFailedEvent.class);
            log.warn("Order CANCELLED | orderId: {} | reason: {}",
                    reply.aggregateId(), failedEvent.reason());
        } else {
            log.warn("Ignoring unsupported inventory reply event type: {}", reply.eventType());
            return;
        }

        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
        log.info("Order status updated in DB | orderId: {} | status: {}",
                order.getOrderId(), order.getStatus());
    }
}
