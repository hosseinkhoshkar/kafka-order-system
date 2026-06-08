package com.example.common.event;

public record InventoryReservationFailedEvent(
        String orderId,
        String productId,
        Integer quantity,
        String reason
) {
}
