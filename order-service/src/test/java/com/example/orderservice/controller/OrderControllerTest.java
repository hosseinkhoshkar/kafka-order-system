package com.example.orderservice.controller;

import com.example.orderservice.config.JsonConfig;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.exception.InvalidIdempotencyKeyException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.service.CreateOrderResult;
import com.example.orderservice.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
@Import(JsonConfig.class)
class OrderControllerTest {
    @Autowired MockMvc mvc;
    @MockBean OrderService service;
    private static final String VALID = """
            {"productId":"p1","customerId":"c1","quantity":2,"price":123456789012345.67}
            """;

    @Test
    void createsResourceWithLocationAndExactDecimal() throws Exception {
        when(service.createOrder(any(), isNull())).thenReturn(CreateOrderResult.created("/api/orders/order-1", response()));
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(VALID))
                .andExpect(status().isCreated()).andExpect(header().string("Location", "/api/orders/order-1"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.currency").value("EUR"));
        verify(service).createOrder(argThat(request -> request.getPrice()
                .equals(new BigDecimal("123456789012345.67"))), isNull());
    }

    @Test
    void forwardsIdempotencyKeyAndMarksReplay() throws Exception {
        when(service.createOrder(any(), eq("demo-key")))
                .thenReturn(CreateOrderResult.replayed(201, "/api/orders/order-1", response()));

        mvc.perform(post("/api/orders")
                        .header("Idempotency-Key", "demo-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotent-Replay", "true"))
                .andExpect(header().string("Location", "/api/orders/order-1"));
    }

    @Test
    void emptyIdempotencyKeyReturnsBadRequest() throws Exception {
        when(service.createOrder(any(), eq("")))
                .thenThrow(new InvalidIdempotencyKeyException("Idempotency-Key must be nonblank"));

        mvc.perform(post("/api/orders")
                        .header("Idempotency-Key", "")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void whitespaceIdempotencyKeyReturnsBadRequest() throws Exception {
        when(service.createOrder(any(), eq("   ")))
                .thenThrow(new InvalidIdempotencyKeyException("Idempotency-Key must be nonblank"));

        mvc.perform(post("/api/orders")
                        .header("Idempotency-Key", "   ")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void readsOrder() throws Exception {
        when(service.getOrder("order-1")).thenReturn(response());
        mvc.perform(get("/api/orders/order-1")).andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("order-1"))
                .andExpect(jsonPath("$.updatedAt").exists());
    }

    @Test
    void unknownOrderIsProblemDetail404() throws Exception {
        when(service.getOrder("missing")).thenThrow(new OrderNotFoundException());
        mvc.perform(get("/api/orders/missing")).andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.detail").value("Order not found"));
    }

    static Stream<String> invalidBodies() {
        return Stream.of("{}", "null", "{",
                VALID.replace("\"quantity\":2", "\"quantity\":0"),
                VALID.replace("\"quantity\":2", "\"quantity\":-1"),
                VALID.replace("\"quantity\":2", "\"quantity\":null"),
                VALID.replace("\"quantity\":2", "\"quantity\":1.5"),
                VALID.replace("\"quantity\":2", "\"quantity\":2147483648"),
                VALID.replace("123456789012345.67", "0"),
                VALID.replace("123456789012345.67", "-1"),
                VALID.replace("123456789012345.67", "null"),
                VALID.replace("123456789012345.67", "0.001"),
                VALID.replace("123456789012345.67", "100000000000000000"),
                VALID.replace("p1", " "), VALID.replace("c1", ""),
                VALID.replace("p1", "p".repeat(256)));
    }

    @ParameterizedTest
    @MethodSource("invalidBodies")
    void rejectsInvalidRequestsBeforeInvokingService(String body) throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400));
        verifyNoInteractions(service);
    }

    @Test
    void validationErrorsIdentifyFields() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.productId").exists())
                .andExpect(jsonPath("$.errors.customerId").exists())
                .andExpect(jsonPath("$.errors.quantity").exists()).andExpect(jsonPath("$.errors.price").exists());
    }

    @Test
    void unexpectedErrorsDoNotExposeInternalDetails() throws Exception {
        when(service.getOrder("order-1")).thenThrow(new IllegalStateException("database-secret"));
        mvc.perform(get("/api/orders/order-1")).andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(content().string(not(containsString("database-secret"))));
    }

    @Test
    void healthRouteStillWorks() throws Exception {
        mvc.perform(get("/api/orders/health")).andExpect(status().isOk())
                .andExpect(content().string("Order Service is running"));
        verifyNoInteractions(service);
    }

    private static OrderResponse response() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 24, 12, 0);
        return new OrderResponse("order-1", "p1", "c1", 2, new BigDecimal("123456789012345.67"),
                "EUR", OrderStatus.PENDING, now, now);
    }
}
