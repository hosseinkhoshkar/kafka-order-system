package com.example.orderservice.config;

import com.example.orderservice.exception.InvalidInventoryReplyException;
import com.example.orderservice.exception.InventoryReplyConflictException;
import org.springframework.kafka.support.serializer.DeserializationException;

final class OrderKafkaErrorPolicy {

    private OrderKafkaErrorPolicy() {
    }

    static boolean isNotRetryable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InvalidInventoryReplyException
                    || current instanceof InventoryReplyConflictException
                    || current instanceof DeserializationException
                    || current instanceof IllegalArgumentException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
