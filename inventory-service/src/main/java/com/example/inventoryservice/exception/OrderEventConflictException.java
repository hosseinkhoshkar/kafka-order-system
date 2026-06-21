package com.example.inventoryservice.exception;

public class OrderEventConflictException extends IllegalArgumentException {
    public OrderEventConflictException(String message) {
        super(message);
    }
}
