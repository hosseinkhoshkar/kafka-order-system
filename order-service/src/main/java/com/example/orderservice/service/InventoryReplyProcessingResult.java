package com.example.orderservice.service;

public record InventoryReplyProcessingResult(String orderId, String outcome) {
    public static InventoryReplyProcessingResult transitioned(String orderId) {
        return new InventoryReplyProcessingResult(orderId, "TRANSITIONED");
    }

    public static InventoryReplyProcessingResult duplicateEvent(String orderId) {
        return new InventoryReplyProcessingResult(orderId, "DUPLICATE_EVENT");
    }

    public static InventoryReplyProcessingResult alreadyFinal(String orderId) {
        return new InventoryReplyProcessingResult(orderId, "ALREADY_FINAL");
    }
}
