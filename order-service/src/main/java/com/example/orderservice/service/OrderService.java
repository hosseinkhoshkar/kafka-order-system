package com.example.orderservice.service;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.entity.OrderIdempotencyRecord;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.exception.IdempotencyConflictException;
import com.example.orderservice.exception.InvalidIdempotencyKeyException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.observability.OrderObservabilityMetrics;
import com.example.orderservice.repository.OrderIdempotencyRecordRepository;
import com.example.orderservice.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private static final String CREATE_ORDER_ENDPOINT = "POST /api/orders";
    private static final int FINGERPRINT_VERSION = 1;

    private final OrderRepository orderRepository;
    private final OrderIdempotencyRecordRepository idempotencyRepository;
    private final EventStoreService eventStoreService;
    private final OutboxEventService outboxEventService;
    private final ObjectMapper objectMapper;
    private final OrderObservabilityMetrics metrics;

    @Transactional
    public CreateOrderResult createOrder(OrderRequest request, String idempotencyKey) {
        if (idempotencyKey == null) {
            OrderResponse response = createNewOrder(request);
            return CreateOrderResult.created("/api/orders/" + response.orderId(), response);
        }

        String normalizedKey = validateIdempotencyKey(idempotencyKey);
        String fingerprint = fingerprint(request);
        int inserted = idempotencyRepository.insertIfAbsent(
                normalizedKey,
                CREATE_ORDER_ENDPOINT,
                fingerprint,
                FINGERPRINT_VERSION,
                LocalDateTime.now()
        );
        if (inserted == 0) {
            return replayOrConflict(normalizedKey, fingerprint);
        }

        OrderResponse response = createNewOrder(request);
        String location = "/api/orders/" + response.orderId();
        idempotencyRepository.markCompleted(
                normalizedKey,
                response.orderId(),
                201,
                location,
                responseBody(response),
                LocalDateTime.now()
        );
        return CreateOrderResult.created(location, response);
    }

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        return createOrder(request, null).body();
    }

    private OrderResponse createNewOrder(OrderRequest request) {
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        try (MDC.MDCCloseable ignored = MDC.putCloseable("orderId", id)) {
            OrderEntity order = OrderEntity.builder()
                    .orderId(id).productId(request.getProductId()).customerId(request.getCustomerId())
                    .quantity(request.getQuantity()).price(request.getPrice())
                    .status(OrderStatus.PENDING).createdAt(now).updatedAt(now).build();
            orderRepository.save(order);

            OrderCreatedEvent payload = new OrderCreatedEvent(id, order.getProductId(), order.getCustomerId(),
                    order.getQuantity(), order.getPrice(), order.getStatus().name(), now);
            EventEnvelope envelope = new EventEnvelope(UUID.randomUUID().toString(), EventTypes.ORDER_CREATED,
                    id, "ORDER", now, 1, id, payload);
            try (MDC.MDCCloseable event = MDC.putCloseable("eventId", envelope.eventId());
                 MDC.MDCCloseable correlation = MDC.putCloseable("correlationId", envelope.correlationId())) {
                eventStoreService.saveEvent(id, "ORDER", EventTypes.ORDER_CREATED, envelope);
                outboxEventService.saveOutboxEvent(id, "ORDER", EventTypes.ORDER_CREATED, envelope);
                metrics.orderCreatedAfterCommit();
                log.info("Order created and queued for publication");
            }
            return response(order);
        }
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String orderId) {
        return response(orderRepository.findById(orderId).orElseThrow(OrderNotFoundException::new));
    }

    private OrderResponse response(OrderEntity order) {
        return new OrderResponse(order.getOrderId(), order.getProductId(), order.getCustomerId(),
                order.getQuantity(), order.getPrice(), "EUR", order.getStatus(),
                order.getCreatedAt(), order.getUpdatedAt());
    }

    private CreateOrderResult replayOrConflict(String key, String fingerprint) {
        OrderIdempotencyRecord record = idempotencyRepository.findById(key)
                .orElseThrow(() -> new IdempotencyConflictException("Idempotency record is not readable yet"));
        if (!CREATE_ORDER_ENDPOINT.equals(record.getEndpoint())
                || record.getFingerprintVersion() != FINGERPRINT_VERSION
                || !record.getRequestFingerprint().equals(fingerprint)) {
            metrics.httpIdempotencyConflict();
            throw new IdempotencyConflictException("Idempotency-Key was already used with a different request");
        }
        if (record.getCompletedAt() == null || record.getResponseBody() == null) {
            metrics.httpIdempotencyConflict();
            throw new IdempotencyConflictException("Idempotent request is still in progress");
        }
        try {
            OrderResponse body = objectMapper.readValue(record.getResponseBody(), OrderResponse.class);
            metrics.httpIdempotencyReplay();
            return CreateOrderResult.replayed(record.getResponseStatus(), record.getResponseLocation(), body);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Stored idempotency response cannot be read", ex);
        }
    }

    private String validateIdempotencyKey(String idempotencyKey) {
        String key = idempotencyKey.trim();
        if (key.isEmpty() || key.length() > 255) {
            throw new InvalidIdempotencyKeyException("Idempotency-Key must be nonblank and at most 255 characters");
        }
        return key;
    }

    private String fingerprint(OrderRequest request) {
        Map<String, Object> canonical = new LinkedHashMap<>();
        canonical.put("productId", request.getProductId());
        canonical.put("customerId", request.getCustomerId());
        canonical.put("quantity", request.getQuantity());
        canonical.put("price", normalizePrice(request.getPrice()));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(objectMapper.writeValueAsBytes(canonical)));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Order request cannot be serialized for idempotency", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 digest is unavailable", ex);
        }
    }

    private String normalizePrice(BigDecimal price) {
        return price.stripTrailingZeros().toPlainString();
    }

    private String responseBody(OrderResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Order response cannot be serialized for idempotency", ex);
        }
    }
}
