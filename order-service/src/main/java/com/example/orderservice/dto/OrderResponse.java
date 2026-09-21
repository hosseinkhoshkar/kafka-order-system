package com.example.orderservice.dto;

import com.example.orderservice.model.OrderStatus;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderResponse(String orderId, String productId, String customerId,
                            Integer quantity, BigDecimal price, String currency,
                            OrderStatus status, LocalDateTime createdAt, LocalDateTime updatedAt) {
}
