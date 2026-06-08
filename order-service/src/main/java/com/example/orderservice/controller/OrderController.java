package com.example.orderservice.controller;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.EventStoreService;
import com.example.orderservice.service.OutboxEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderRepository orderRepository;
    private final EventStoreService eventStoreService;
    private final OutboxEventService outboxEventService;

    @PostMapping
    @Transactional
    public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        OrderEntity orderEntity = OrderEntity.builder()
                .orderId(orderId)
                .productId(request.getProductId())
                .customerId(request.getCustomerId())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .status(OrderStatus.PENDING)
                .createdAt(now)
                .updatedAt(now)
                .build();
        orderRepository.save(orderEntity);
        log.info("Order saved to DB | orderId: {}", orderId);

        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                orderId,
                request.getProductId(),
                request.getCustomerId(),
                request.getQuantity(),
                BigDecimal.valueOf(request.getPrice()),
                OrderStatus.PENDING.name(),
                now
        );
        EventEnvelope eventEnvelope = new EventEnvelope(
                UUID.randomUUID().toString(),
                EventTypes.ORDER_CREATED,
                orderId,
                "ORDER",
                now,
                1,
                orderId,
                orderCreatedEvent
        );

        eventStoreService.saveEvent(orderId, "ORDER", EventTypes.ORDER_CREATED, eventEnvelope);
        outboxEventService.saveOutboxEvent(orderId, "ORDER", EventTypes.ORDER_CREATED, eventEnvelope);

        Order order = Order.builder()
                .orderId(orderId)
                .productId(request.getProductId())
                .customerId(request.getCustomerId())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .status(OrderStatus.PENDING.name())
                .createdAt(now)
                .build();

        log.info("New order created: {}", orderId);
        return ResponseEntity.ok(order);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Order Service is running");
    }
}
