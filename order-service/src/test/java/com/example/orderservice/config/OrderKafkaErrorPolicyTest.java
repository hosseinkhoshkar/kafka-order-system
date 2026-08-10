package com.example.orderservice.config;

import com.example.orderservice.exception.InvalidInventoryReplyException;
import com.example.orderservice.exception.InventoryReplyConflictException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

import static org.assertj.core.api.Assertions.assertThat;

class OrderKafkaErrorPolicyTest {

    @Test
    void permanentContractErrorsAreNotRetryable() {
        assertThat(OrderKafkaErrorPolicy.isNotRetryable(
                new InvalidInventoryReplyException("bad payload"))).isTrue();
        assertThat(OrderKafkaErrorPolicy.isNotRetryable(
                new InventoryReplyConflictException("final status conflict"))).isTrue();
    }

    @Test
    void transientInfrastructureErrorsRemainRetryable() {
        assertThat(OrderKafkaErrorPolicy.isNotRetryable(
                new TransientDataAccessResourceException("temporary db outage"))).isFalse();
    }
}
