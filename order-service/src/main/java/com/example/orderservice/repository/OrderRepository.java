package com.example.orderservice.repository;

import com.example.orderservice.entity.OrderEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select order from OrderEntity order where order.orderId = :orderId")
    Optional<OrderEntity> findByIdForUpdate(@Param("orderId") String orderId);
}
