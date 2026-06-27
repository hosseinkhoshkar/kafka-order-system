package com.example.orderservice.repository;

import com.example.orderservice.entity.OrderInboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface OrderInboxEventRepository extends JpaRepository<OrderInboxEvent, String> {

    @Modifying
    @Query(value = """
            INSERT INTO order_inbox_events(event_id, event_type, aggregate_id, payload_hash, received_at)
            VALUES (:eventId, :eventType, :aggregateId, :payloadHash, :receivedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") String eventId,
                       @Param("eventType") String eventType,
                       @Param("aggregateId") String aggregateId,
                       @Param("payloadHash") String payloadHash,
                       @Param("receivedAt") LocalDateTime receivedAt);
}
