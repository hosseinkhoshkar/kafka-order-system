package com.example.orderservice.exception;

public class InvalidInventoryReplyException extends IllegalArgumentException {
    public InvalidInventoryReplyException(String message) {
        super(message);
    }

    public InvalidInventoryReplyException(String message, Throwable cause) {
        super(message, cause);
    }
}
