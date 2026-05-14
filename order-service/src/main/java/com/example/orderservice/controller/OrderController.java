package com.example.orderservice.controller;

import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.producer.OrderProducer;
import com.example.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderRepository orderRepository;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        // ۱. ذخیره داخل DB
        OrderEntity orderEntity = OrderEntity.builder()
                .orderId(orderId)
                .productId(request.getProductId())
                .customerId(request.getCustomerId())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .status(OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
        orderRepository.save(orderEntity);
        log.info("💾 Order saved to DB | orderId: {}", orderId);

        // ۲. ارسال به Kafka
        Order order = Order.builder()
                .orderId(orderId)
                .productId(request.getProductId())
                .customerId(request.getCustomerId())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .status(OrderStatus.PENDING.name())
                .createdAt(LocalDateTime.now())
                .build();
        orderProducer.sendOrder(order);
        log.info("📦 New order created: {}", orderId);

        return ResponseEntity.ok(order);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Order Service is running ✅");
    }
}