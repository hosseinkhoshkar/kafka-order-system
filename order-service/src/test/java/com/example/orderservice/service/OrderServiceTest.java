package com.example.orderservice.service;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.entity.OrderIdempotencyRecord;
import com.example.orderservice.exception.IdempotencyConflictException;
import com.example.orderservice.exception.InvalidIdempotencyKeyException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.observability.OrderObservabilityMetrics;
import com.example.orderservice.repository.OrderIdempotencyRecordRepository;
import com.example.orderservice.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {
    @Mock OrderRepository repository;
    @Mock OrderIdempotencyRecordRepository idempotencyRepository;
    @Mock EventStoreService history;
    @Mock OutboxEventService outbox;
    @Mock OrderObservabilityMetrics metrics;
    ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    OrderService service;

    @BeforeEach
    void setUp() {
        service = new OrderService(repository, idempotencyRepository, history, outbox, objectMapper, metrics);
    }

    @Test
    void creationPreservesMoneyAndEventIdentityAcrossWrites() {
        var request = new OrderRequest();
        request.setProductId("p1"); request.setCustomerId("c1"); request.setQuantity(2);
        request.setPrice(new BigDecimal("123456789012345.67"));
        var result = service.createOrder(request);
        ArgumentCaptor<OrderEntity> entity = ArgumentCaptor.forClass(OrderEntity.class);
        ArgumentCaptor<EventEnvelope> envelope = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(repository).save(entity.capture());
        verify(outbox).saveOutboxEvent(eq(result.orderId()), eq("ORDER"),
                eq(EventTypes.ORDER_CREATED), envelope.capture());
        verify(history).saveEvent(result.orderId(), "ORDER", EventTypes.ORDER_CREATED, envelope.getValue());
        var event = (OrderCreatedEvent) envelope.getValue().payload();
        assertThat(result.price()).isEqualByComparingTo(request.getPrice());
        assertThat(entity.getValue().getPrice()).isEqualByComparingTo(request.getPrice());
        assertThat(event.price()).isEqualByComparingTo(request.getPrice());
        assertThat(event.orderId()).isEqualTo(result.orderId());
        assertThat(envelope.getValue().correlationId()).isEqualTo(result.orderId());
        assertThat(result.status()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void missingIdempotencyKeyCreatesOrderWithoutIdempotencyRecord() {
        CreateOrderResult result = service.createOrder(validRequest(), null);

        assertThat(result.statusCode()).isEqualTo(201);
        assertThat(result.replayed()).isFalse();
        verify(repository).save(any(OrderEntity.class));
        verifyNoInteractions(idempotencyRepository);
    }

    @Test
    void blankIdempotencyKeyIsRejectedBeforePersistence() {
        var request = validRequest();

        assertThatThrownBy(() -> service.createOrder(request, "   "))
                .isInstanceOf(InvalidIdempotencyKeyException.class);

        verifyNoInteractions(repository, idempotencyRepository, history, outbox);
    }

    @Test
    void emptyIdempotencyKeyIsRejectedBeforePersistence() {
        var request = validRequest();

        assertThatThrownBy(() -> service.createOrder(request, ""))
                .isInstanceOf(InvalidIdempotencyKeyException.class);

        verifyNoInteractions(repository, idempotencyRepository, history, outbox);
    }

    @Test
    void idempotencyKeyReplaysStoredResponseForSameRequest() throws Exception {
        var request = validRequest();
        var stored = response("order-1");
        when(idempotencyRepository.insertIfAbsent(anyString(), anyString(), anyString(), anyInt(), any()))
                .thenReturn(0);
        when(idempotencyRepository.findById("key-1")).thenReturn(Optional.of(OrderIdempotencyRecord.builder()
                .idempotencyKey("key-1")
                .endpoint("POST /api/orders")
                .requestFingerprint(fingerprintFor(request))
                .fingerprintVersion(1)
                .responseStatus(201)
                .responseLocation("/api/orders/order-1")
                .responseBody(objectMapper.writeValueAsString(stored))
                .createdAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .build()));

        CreateOrderResult result = service.createOrder(request, "key-1");

        assertThat(result.replayed()).isTrue();
        assertThat(result.body().orderId()).isEqualTo("order-1");
        verifyNoInteractions(repository, history, outbox);
        verify(metrics).httpIdempotencyReplay();
    }

    @Test
    void idempotencyKeyConflictDoesNotCreateOrder() {
        var request = validRequest();
        when(idempotencyRepository.insertIfAbsent(anyString(), anyString(), anyString(), anyInt(), any()))
                .thenReturn(0);
        when(idempotencyRepository.findById("key-1")).thenReturn(Optional.of(OrderIdempotencyRecord.builder()
                .idempotencyKey("key-1")
                .endpoint("POST /api/orders")
                .requestFingerprint("different")
                .fingerprintVersion(1)
                .createdAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .build()));

        assertThatThrownBy(() -> service.createOrder(request, "key-1"))
                .isInstanceOf(IdempotencyConflictException.class);

        verifyNoInteractions(repository, history, outbox);
        verify(metrics).httpIdempotencyConflict();
    }

    @Test
    void readsPersistedFinalStatusWithoutPublishing() {
        when(repository.findById("o1")).thenReturn(Optional.of(OrderEntity.builder()
                .orderId("o1").price(new BigDecimal("0.10")).status(OrderStatus.CONFIRMED).build()));
        var result = service.getOrder("o1");
        assertThat(result.status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(result.price()).isEqualByComparingTo("0.10");
        verifyNoInteractions(history, outbox);
    }

    @Test
    void missingOrderThrowsDomainException() {
        when(repository.findById("missing")).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.getOrder("missing")).isInstanceOf(OrderNotFoundException.class);
        verifyNoInteractions(history, outbox);
    }

    private OrderRequest validRequest() {
        var request = new OrderRequest();
        request.setProductId("p1");
        request.setCustomerId("c1");
        request.setQuantity(2);
        request.setPrice(new BigDecimal("12.50"));
        return request;
    }

    private OrderResponse response(String orderId) {
        LocalDateTime now = LocalDateTime.of(2026, 9, 24, 12, 0);
        return new OrderResponse(orderId, "p1", "c1", 2, new BigDecimal("12.50"),
                "EUR", OrderStatus.PENDING, now, now);
    }

    private String fingerprintFor(OrderRequest request) throws Exception {
        var method = OrderService.class.getDeclaredMethod("fingerprint", OrderRequest.class);
        method.setAccessible(true);
        return (String) method.invoke(service, request);
    }
}
