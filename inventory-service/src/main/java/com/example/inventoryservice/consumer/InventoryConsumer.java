package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryConsumer {

    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "inventory-group"
    )
    public void consumeOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();

        log.info("📥 Order received | id: {} | product: {} | qty: {} | partition: {} | offset: {}",
                order.getOrderId(),
                order.getProductId(),
                order.getQuantity(),
                record.partition(),
                record.offset()
        );

        processInventory(order);
    }

    // Dead Letter Topic consumer — پیام های failed رو اینجا می‌گیریم
    @KafkaListener(
            topics = "orders.DLT",
            groupId = "inventory-group-dlt"
    )
    public void consumeDeadLetter(ConsumerRecord<String, Order> record) {
        log.error("💀 Dead Letter received | id: {} | product: {} | reason: failed after retries",
                record.value().getOrderId(),
                record.value().getProductId()
        );
        // اینجا می‌تونی alert بفرستی، داخل DB ذخیره کنی و ...
    }

    private void processInventory(Order order) {
        log.info("🔄 Processing inventory for product: {} | quantity: {}",
                order.getProductId(),
                order.getQuantity()
        );

        // شبیه‌سازی خطا — اگه quantity بیشتر از 10 بود خطا بده
        if (order.getQuantity() > 10) {
            log.warn("⚠️ Insufficient inventory for order: {}", order.getOrderId());
            throw new RuntimeException("Insufficient inventory for product: " + order.getProductId());
        }

        log.info("✅ Inventory OK for order: {}", order.getOrderId());
    }
}