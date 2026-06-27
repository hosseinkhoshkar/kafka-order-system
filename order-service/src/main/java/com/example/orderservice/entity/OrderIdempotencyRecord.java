package com.example.orderservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "order_idempotency_keys")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderIdempotencyRecord {

    @Id
    private String idempotencyKey;

    @Column(nullable = false)
    private String endpoint;

    @Column(nullable = false, length = 64)
    private String requestFingerprint;

    @Column(nullable = false)
    private Integer fingerprintVersion;

    private String orderId;
    private Integer responseStatus;
    private String responseLocation;

    @Column(columnDefinition = "TEXT")
    private String responseBody;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime completedAt;
}
