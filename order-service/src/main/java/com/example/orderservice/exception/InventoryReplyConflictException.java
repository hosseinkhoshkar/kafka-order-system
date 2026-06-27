package com.example.orderservice.exception;

public class InventoryReplyConflictException extends IllegalArgumentException {
    public InventoryReplyConflictException(String message) {
        super(message);
    }
}
