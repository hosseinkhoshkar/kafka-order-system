package com.example.orderservice.exception;

public class InvalidIdempotencyKeyException extends IllegalArgumentException {
    public InvalidIdempotencyKeyException(String message) {
        super(message);
    }
}
