package com.example.orderservice.service;

import com.example.orderservice.dto.OrderResponse;

public record CreateOrderResult(int statusCode, String location, OrderResponse body, boolean replayed) {
    public static CreateOrderResult created(String location, OrderResponse body) {
        return new CreateOrderResult(201, location, body, false);
    }

    public static CreateOrderResult replayed(int statusCode, String location, OrderResponse body) {
        return new CreateOrderResult(statusCode, location, body, true);
    }
}
