package com.example.inventoryservice.config;

import com.example.inventoryservice.exception.InvalidInventoryMessageException;
import com.example.inventoryservice.exception.OrderEventConflictException;
import org.springframework.kafka.support.serializer.DeserializationException;

final class InventoryKafkaErrorPolicy {

    private InventoryKafkaErrorPolicy() {
    }

    static boolean isNotRetryable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InvalidInventoryMessageException
                    || current instanceof OrderEventConflictException
                    || current instanceof DeserializationException
                    || current instanceof IllegalArgumentException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
