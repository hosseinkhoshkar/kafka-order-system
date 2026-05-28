package com.example.orderservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private String id;

    private String aggregateId;    // orderId
    private String aggregateType;  // ORDER
    private String eventType;      // ORDER_CREATED

    @Column(columnDefinition = "TEXT")
    private String payload;        // JSON کامل

    private String status;         // PENDING, SENT, FAILED

    private LocalDateTime createdAt;
    private LocalDateTime sentAt;
}