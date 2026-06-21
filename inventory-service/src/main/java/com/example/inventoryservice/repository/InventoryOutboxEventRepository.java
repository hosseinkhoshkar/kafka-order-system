package com.example.inventoryservice.repository;

import com.example.inventoryservice.entity.InventoryOutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface InventoryOutboxEventRepository extends JpaRepository<InventoryOutboxEvent, String> {

    @Query(value = """
            WITH candidates AS (
                SELECT id
                FROM inventory_outbox_events
                WHERE (
                    status = 'PENDING'
                    AND next_attempt_at <= :now
                    AND attempt_count < :maxAttempts
                ) OR (
                    status = 'IN_PROGRESS'
                    AND claimed_until < :now
                    AND attempt_count < :maxAttempts
                )
                ORDER BY created_at, id
                LIMIT :batchSize
                FOR UPDATE SKIP LOCKED
            )
            UPDATE inventory_outbox_events event
            SET status = 'IN_PROGRESS',
                claim_token = :claimToken,
                claimed_by = :claimedBy,
                claimed_until = :claimedUntil,
                attempt_count = event.attempt_count + 1,
                last_error = NULL
            FROM candidates
            WHERE event.id = candidates.id
            RETURNING event.*
            """, nativeQuery = true)
    List<InventoryOutboxEvent> claimAvailable(@Param("now") LocalDateTime now,
                                              @Param("claimedUntil") LocalDateTime claimedUntil,
                                              @Param("claimToken") String claimToken,
                                              @Param("claimedBy") String claimedBy,
                                              @Param("batchSize") int batchSize,
                                              @Param("maxAttempts") int maxAttempts);

    @Modifying
    @Query("""
            update InventoryOutboxEvent event
            set event.status = 'SENT',
                event.sentAt = :sentAt,
                event.claimToken = null,
                event.claimedBy = null,
                event.claimedUntil = null,
                event.lastError = null
            where event.id = :id
              and event.claimToken = :claimToken
              and event.status = 'IN_PROGRESS'
            """)
    int markSent(@Param("id") String id,
                 @Param("claimToken") String claimToken,
                 @Param("sentAt") LocalDateTime sentAt);

    @Modifying
    @Query("""
            update InventoryOutboxEvent event
            set event.status = :status,
                event.nextAttemptAt = :nextAttemptAt,
                event.lastError = :lastError,
                event.claimToken = null,
                event.claimedBy = null,
                event.claimedUntil = null
            where event.id = :id
              and event.claimToken = :claimToken
              and event.status = 'IN_PROGRESS'
            """)
    int markPublishFailure(@Param("id") String id,
                           @Param("claimToken") String claimToken,
                           @Param("status") String status,
                           @Param("nextAttemptAt") LocalDateTime nextAttemptAt,
                           @Param("lastError") String lastError);

    @Modifying
    @Query("""
            update InventoryOutboxEvent event
            set event.status = 'FAILED',
                event.lastError = :lastError,
                event.claimToken = null,
                event.claimedBy = null,
                event.claimedUntil = null
            where event.status in ('PENDING', 'IN_PROGRESS')
              and event.attemptCount >= :maxAttempts
            """)
    int markAttemptsExhausted(@Param("maxAttempts") int maxAttempts,
                              @Param("lastError") String lastError);
}
