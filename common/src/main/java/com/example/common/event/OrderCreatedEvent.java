package com.example.common.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderCreatedEvent(
        String orderId,
        String productId,
        String customerId,
        Integer quantity,
        BigDecimal price,
        String status,
        LocalDateTime createdAt
) {
}
