package com.example.orderservice.repository;

import com.example.orderservice.entity.OrderIdempotencyRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface OrderIdempotencyRecordRepository extends JpaRepository<OrderIdempotencyRecord, String> {

    @Modifying
    @Query(value = """
            INSERT INTO order_idempotency_keys(
                idempotency_key, endpoint, request_fingerprint, fingerprint_version, created_at)
            VALUES (:key, :endpoint, :fingerprint, :fingerprintVersion, :createdAt)
            ON CONFLICT (idempotency_key) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("key") String key,
                       @Param("endpoint") String endpoint,
                       @Param("fingerprint") String fingerprint,
                       @Param("fingerprintVersion") int fingerprintVersion,
                       @Param("createdAt") LocalDateTime createdAt);

    @Modifying
    @Query("""
            update OrderIdempotencyRecord record
            set record.orderId = :orderId,
                record.responseStatus = :responseStatus,
                record.responseLocation = :responseLocation,
                record.responseBody = :responseBody,
                record.completedAt = :completedAt
            where record.idempotencyKey = :key
            """)
    int markCompleted(@Param("key") String key,
                      @Param("orderId") String orderId,
                      @Param("responseStatus") int responseStatus,
                      @Param("responseLocation") String responseLocation,
                      @Param("responseBody") String responseBody,
                      @Param("completedAt") LocalDateTime completedAt);
}
