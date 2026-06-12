package com.example.orderservice.dto;

import jakarta.validation.constraints.*;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderRequest {
    @NotBlank
    @Size(max = 255)
    private String productId;
    @NotBlank
    @Size(max = 255)
    private String customerId;
    @NotNull
    @Positive
    private Integer quantity;
    // Unit price in EUR; no implicit rounding of incoming amounts.
    @NotNull
    @DecimalMin(value = "0.00", inclusive = false)
    @Digits(integer = 17, fraction = 2)
    private BigDecimal price;
}
