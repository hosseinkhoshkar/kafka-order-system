package com.example.orderservice.consumer;

import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.model.InventoryReply;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.EventStoreService;
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

    @KafkaListener(topics = "${kafka.topic.inventory-reply}", groupId = "order-group")
    public void consumeInventoryReply(ConsumerRecord<String, InventoryReply> record) {
        InventoryReply reply = record.value();

        log.info("📨 Inventory reply received | orderId: {} | status: {}",
                reply.getOrderId(), reply.getStatus());

        Optional<OrderEntity> orderOpt = orderRepository.findById(reply.getOrderId());

        if (orderOpt.isEmpty()) {
            log.error("❌ Order not found in DB | orderId: {}", reply.getOrderId());
            return;
        }

        OrderEntity order = orderOpt.get();

        if (reply.getStatus() == OrderStatus.INVENTORY_RESERVED) {
            order.setStatus(OrderStatus.CONFIRMED);
            eventStoreService.saveEvent(order.getOrderId(), "ORDER", "ORDER_CONFIRMED", reply);
            log.info("✅ Order CONFIRMED | orderId: {}", reply.getOrderId());
        } else if (reply.getStatus() == OrderStatus.INVENTORY_FAILED) {
            order.setStatus(OrderStatus.CANCELLED);
            eventStoreService.saveEvent(order.getOrderId(), "ORDER", "ORDER_CANCELLED", reply);
            log.warn("❌ Order CANCELLED | orderId: {} | reason: {}",
                    reply.getOrderId(), reply.getMessage());
        }

        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
        log.info("💾 Order status updated in DB | orderId: {} | status: {}",
                order.getOrderId(), order.getStatus());
    }
}