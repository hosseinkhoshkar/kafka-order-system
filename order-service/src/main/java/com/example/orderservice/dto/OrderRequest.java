package com.example.orderservice.dto;

import lombok.Data;

@Data
public class OrderRequest {
    private String productId;
    private String customerId;
    private Integer quantity;
    private Double price;
}