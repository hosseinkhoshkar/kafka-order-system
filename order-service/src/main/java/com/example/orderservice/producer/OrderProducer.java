package com.example.orderservice.producer;

import com.example.orderservice.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send(ordersTopic, order.getOrderId(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ Order sent successfully | orderId: {} | partition: {} | offset: {}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                );
            } else {
                log.error("❌ Failed to send order | orderId: {} | error: {}",
                        order.getOrderId(), ex.getMessage());
            }
        });
    }
}