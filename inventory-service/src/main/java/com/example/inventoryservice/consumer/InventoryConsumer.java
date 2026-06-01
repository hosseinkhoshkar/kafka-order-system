package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.OrderStatus;
import com.example.inventoryservice.model.Order;
import com.example.inventoryservice.model.InventoryReply;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.inventory-reply}")
    private String inventoryReplyTopic;

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "inventory-group")
    public void consumeOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();

        log.info("📥 Order received | id: {} | product: {} | qty: {} | partition: {} | offset: {}",
                order.getOrderId(),
                order.getProductId(),
                order.getQuantity(),
                record.partition(),
                record.offset()
        );

        InventoryReply reply = processInventory(order);

        kafkaTemplate.send(inventoryReplyTopic, order.getOrderId(), reply);
        log.info("📤 Reply sent | orderId: {} | status: {}",
                order.getOrderId(), reply.getStatus());
    }

    @KafkaListener(topics = "orders.DLT", groupId = "inventory-group-dlt")
    public void consumeDeadLetter(ConsumerRecord<String, Order> record) {
        log.error("💀 Dead Letter received | id: {} | product: {} | reason: failed after retries",
                record.value().getOrderId(),
                record.value().getProductId()
        );
    }

    private InventoryReply processInventory(Order order) {
        log.info("🔄 Processing inventory for product: {} | quantity: {}",
                order.getProductId(),
                order.getQuantity()
        );

        if (order.getQuantity() > 10) {
            log.warn("⚠️ Insufficient inventory for order: {}", order.getOrderId());
            return InventoryReply.builder()
                    .orderId(order.getOrderId())
                    .status(OrderStatus.INVENTORY_FAILED)
                    .message("Insufficient inventory for: " + order.getProductId())
                    .build();
        }

        log.info("✅ Inventory OK for order: {}", order.getOrderId());
        return InventoryReply.builder()
                .orderId(order.getOrderId())
                .status(OrderStatus.INVENTORY_RESERVED)
                .message("Inventory reserved for: " + order.getProductId())
                .build();
    }
}