package com.example.inventoryservice.service;

public record InventoryProcessingResult(String orderId, String outcome) {
    public static InventoryProcessingResult created(String orderId) {
        return new InventoryProcessingResult(orderId, "CREATED");
    }

    public static InventoryProcessingResult duplicateEvent(String orderId) {
        return new InventoryProcessingResult(orderId, "DUPLICATE_EVENT");
    }

    public static InventoryProcessingResult existingDecision(String orderId) {
        return new InventoryProcessingResult(orderId, "EXISTING_DECISION");
    }
}
