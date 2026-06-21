package com.example.inventoryservice.repository;

import com.example.inventoryservice.entity.InventoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface InventoryRepository extends JpaRepository<InventoryEntity, String> {
    @Modifying
    @Query(value = """
            UPDATE inventory
            SET available_quantity = available_quantity - :quantity,
                reserved_quantity = reserved_quantity + :quantity
            WHERE product_id = :productId
              AND available_quantity >= :quantity
              AND reserved_quantity <= 2147483647 - :quantity
            """, nativeQuery = true)
    int reserveIfAvailable(@Param("productId") String productId,
                           @Param("quantity") int quantity);
}
