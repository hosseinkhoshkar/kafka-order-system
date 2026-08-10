package com.example.inventoryservice.config;

import com.example.inventoryservice.exception.InvalidInventoryMessageException;
import com.example.inventoryservice.exception.OrderEventConflictException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryKafkaErrorPolicyTest {

    @Test
    void permanentContractErrorsAreNotRetryable() {
        assertThat(InventoryKafkaErrorPolicy.isNotRetryable(
                new InvalidInventoryMessageException("bad payload"))).isTrue();
        assertThat(InventoryKafkaErrorPolicy.isNotRetryable(
                new OrderEventConflictException("same order different product"))).isTrue();
    }

    @Test
    void transientInfrastructureErrorsRemainRetryable() {
        assertThat(InventoryKafkaErrorPolicy.isNotRetryable(
                new TransientDataAccessResourceException("temporary db outage"))).isFalse();
    }
}
