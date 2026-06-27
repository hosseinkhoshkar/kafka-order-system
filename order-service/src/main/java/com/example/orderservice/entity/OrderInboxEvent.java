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
@Table(name = "order_inbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderInboxEvent {

    @Id
    private String eventId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false, length = 64)
    private String payloadHash;

    @Column(nullable = false)
    private LocalDateTime receivedAt;
}
