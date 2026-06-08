package com.example.common.event;

public record InventoryReservedEvent(
        String orderId,
        String productId,
        Integer quantity,
        String message
) {
}
