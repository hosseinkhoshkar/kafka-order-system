package com.example.inventoryservice.repository;

import com.example.inventoryservice.entity.InventoryReservationDecision;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface InventoryReservationDecisionRepository extends JpaRepository<InventoryReservationDecision, String> {

    @Query(value = "SELECT pg_advisory_xact_lock(hashtext(:orderId))", nativeQuery = true)
    void lockOrder(@Param("orderId") String orderId);
}
