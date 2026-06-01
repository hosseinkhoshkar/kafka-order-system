package com.example.orderservice.consumer;

import com.example.orderservice.model.InventoryReply;
import com.example.orderservice.model.OrderStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SagaReplyConsumer {

    @KafkaListener(topics = "${kafka.topic.inventory-reply}", groupId = "order-group")
    public void consumeInventoryReply(ConsumerRecord<String, InventoryReply> record) {
        InventoryReply reply = record.value();

        log.info("📨 Inventory reply received | orderId: {} | status: {} | partition: {} | offset: {}",
                reply.getOrderId(),
                reply.getStatus(),
                record.partition(),
                record.offset()
        );

        if (reply.getStatus() == OrderStatus.INVENTORY_RESERVED) {
            handleOrderConfirmed(reply);
        } else if (reply.getStatus() == OrderStatus.INVENTORY_FAILED) {
            handleOrderCancelled(reply);
        }
    }

    private void handleOrderConfirmed(InventoryReply reply) {
        log.info("✅ Order CONFIRMED | orderId: {} | message: {}",
                reply.getOrderId(),
                reply.getMessage()
        );
        // TODO: آپدیت status سفارش به CONFIRMED داخل DB
    }

    private void handleOrderCancelled(InventoryReply reply) {
        log.warn("❌ Order CANCELLED | orderId: {} | reason: {}",
                reply.getOrderId(),
                reply.getMessage()
        );
        // TODO: آپدیت status سفارش به CANCELLED داخل DB
        // TODO: Compensating Transaction — مثلاً برگشت پول
    }
}