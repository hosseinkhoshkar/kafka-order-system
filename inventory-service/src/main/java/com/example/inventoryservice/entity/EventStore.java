package com.example.inventoryservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Table(name = "event_store")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventStore {

    @Id
    private String id;

    private String aggregateId;
    private String aggregateType;
    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private Integer version;

    private LocalDateTime occurredAt;
}