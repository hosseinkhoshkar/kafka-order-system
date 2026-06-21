package com.example.inventoryservice.entity;

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
@Table(name = "inventory_reservation_decisions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservationDecision {

    @Id
    private String orderId;

    @Column(nullable = false)
    private String sourceEventId;

    @Column(nullable = false)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(nullable = false)
    private String status;

    @Column(length = 512)
    private String failureReason;

    @Column(nullable = false)
    private String responseEventId;

    @Column(nullable = false)
    private LocalDateTime decidedAt;
}
