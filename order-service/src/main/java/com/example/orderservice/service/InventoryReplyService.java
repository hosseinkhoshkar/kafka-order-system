package com.example.orderservice.service;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventSchemaVersions;
import com.example.common.event.EventTypes;
import com.example.common.event.InventoryReservationFailedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.example.orderservice.entity.OrderEntity;
import com.example.orderservice.exception.InvalidInventoryReplyException;
import com.example.orderservice.exception.InventoryReplyConflictException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderInboxEventRepository;
import com.example.orderservice.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class InventoryReplyService {

    private final OrderInboxEventRepository inboxRepository;
    private final OrderRepository orderRepository;
    private final EventStoreService eventStoreService;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    @Transactional
    public InventoryReplyProcessingResult process(EventEnvelope envelope) {
        validateEnvelope(envelope);
        ReplyDetails details = readAndValidatePayload(envelope);
        String payloadHash = hash(envelope);

        int inserted = inboxRepository.insertIfAbsent(
                envelope.eventId(),
                envelope.eventType(),
                envelope.aggregateId(),
                payloadHash,
                LocalDateTime.now(clock)
        );
        if (inserted == 0) {
            var existing = inboxRepository.findById(envelope.eventId())
                    .orElseThrow(() -> new IllegalStateException("Order inbox conflict did not return existing event"));
            if (!existing.getPayloadHash().equals(payloadHash)) {
                throw new InvalidInventoryReplyException(
                        "Duplicate inventory reply eventId has different content: " + envelope.eventId());
            }
            return InventoryReplyProcessingResult.duplicateEvent(details.orderId());
        }

        OrderEntity order = orderRepository.findByIdForUpdate(details.orderId())
                .orElseThrow(OrderNotFoundException::new);
        validateAgainstOrder(order, details);

        OrderStatus targetStatus = targetStatus(envelope.eventType());
        String orderEventType = orderEventType(envelope.eventType());
        if (order.getStatus() == OrderStatus.PENDING) {
            order.setStatus(targetStatus);
            order.setUpdatedAt(LocalDateTime.now(clock));
            orderRepository.save(order);
            eventStoreService.saveEvent(order.getOrderId(), "ORDER", orderEventType, envelope);
            return InventoryReplyProcessingResult.transitioned(order.getOrderId());
        }

        if (order.getStatus() == targetStatus) {
            return InventoryReplyProcessingResult.alreadyFinal(order.getOrderId());
        }

        throw new InventoryReplyConflictException(
                "Inventory reply " + envelope.eventId() + " conflicts with final order status " + order.getStatus());
    }

    private void validateEnvelope(EventEnvelope envelope) {
        if (envelope == null) {
            throw new InvalidInventoryReplyException("Inventory reply envelope is required");
        }
        requireText(envelope.eventId(), "eventId");
        requireText(envelope.aggregateId(), "aggregateId");
        if (!EventTypes.INVENTORY_RESERVED.equals(envelope.eventType())
                && !EventTypes.INVENTORY_RESERVATION_FAILED.equals(envelope.eventType())) {
            throw new InvalidInventoryReplyException("Unsupported inventory reply type: " + envelope.eventType());
        }
        if (envelope.schemaVersion() != EventSchemaVersions.V1) {
            throw new InvalidInventoryReplyException("Unsupported inventory reply schema version: " + envelope.schemaVersion());
        }
        if (envelope.payload() == null) {
            throw new InvalidInventoryReplyException("Inventory reply payload is required");
        }
    }

    private ReplyDetails readAndValidatePayload(EventEnvelope envelope) {
        try {
            if (EventTypes.INVENTORY_RESERVED.equals(envelope.eventType())) {
                InventoryReservedEvent payload = objectMapper.convertValue(envelope.payload(), InventoryReservedEvent.class);
                validatePayloadFields(envelope, payload.orderId(), payload.productId(), payload.quantity());
                return new ReplyDetails(payload.orderId(), payload.productId(), payload.quantity());
            }
            InventoryReservationFailedEvent payload =
                    objectMapper.convertValue(envelope.payload(), InventoryReservationFailedEvent.class);
            validatePayloadFields(envelope, payload.orderId(), payload.productId(), payload.quantity());
            return new ReplyDetails(payload.orderId(), payload.productId(), payload.quantity());
        } catch (IllegalArgumentException ex) {
            throw new InvalidInventoryReplyException("Invalid inventory reply payload", ex);
        }
    }

    private void validatePayloadFields(EventEnvelope envelope, String orderId, String productId, Integer quantity) {
        requireText(orderId, "payload.orderId");
        requireText(productId, "payload.productId");
        if (!envelope.aggregateId().equals(orderId)) {
            throw new InvalidInventoryReplyException("aggregateId must match payload.orderId");
        }
        if (quantity == null || quantity <= 0) {
            throw new InvalidInventoryReplyException("payload.quantity must be a positive integer");
        }
    }

    private void validateAgainstOrder(OrderEntity order, ReplyDetails details) {
        if (!Objects.equals(order.getProductId(), details.productId())
                || !Objects.equals(order.getQuantity(), details.quantity())) {
            throw new InventoryReplyConflictException(
                    "Inventory reply productId or quantity does not match order " + order.getOrderId());
        }
    }

    private OrderStatus targetStatus(String eventType) {
        return EventTypes.INVENTORY_RESERVED.equals(eventType) ? OrderStatus.CONFIRMED : OrderStatus.CANCELLED;
    }

    private String orderEventType(String eventType) {
        return EventTypes.INVENTORY_RESERVED.equals(eventType) ? "ORDER_CONFIRMED" : "ORDER_CANCELLED";
    }

    private String hash(EventEnvelope envelope) {
        try {
            byte[] json = objectMapper.writeValueAsBytes(envelope);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(json));
        } catch (JsonProcessingException ex) {
            throw new InvalidInventoryReplyException("Inventory reply cannot be serialized for inbox hashing", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 digest is unavailable", ex);
        }
    }

    private void requireText(String value, String field) {
        if (!StringUtils.hasText(value)) {
            throw new InvalidInventoryReplyException(field + " is required");
        }
    }

    private record ReplyDetails(String orderId, String productId, Integer quantity) {
    }
}
