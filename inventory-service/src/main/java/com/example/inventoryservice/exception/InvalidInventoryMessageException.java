package com.example.inventoryservice.exception;

public class InvalidInventoryMessageException extends IllegalArgumentException {
    public InvalidInventoryMessageException(String message) {
        super(message);
    }

    public InvalidInventoryMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
