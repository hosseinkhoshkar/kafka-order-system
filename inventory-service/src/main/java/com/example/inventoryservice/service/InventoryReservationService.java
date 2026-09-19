package com.example.inventoryservice.service;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventSchemaVersions;
import com.example.common.event.EventTypes;
import com.example.common.event.InventoryReservationFailedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.example.common.event.OrderCreatedEvent;
import com.example.inventoryservice.entity.InventoryInboxEvent;
import com.example.inventoryservice.entity.InventoryReservationDecision;
import com.example.inventoryservice.exception.InvalidInventoryMessageException;
import com.example.inventoryservice.exception.OrderEventConflictException;
import com.example.inventoryservice.observability.InventoryObservabilityMetrics;
import com.example.inventoryservice.repository.InventoryInboxEventRepository;
import com.example.inventoryservice.repository.InventoryRepository;
import com.example.inventoryservice.repository.InventoryReservationDecisionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryReservationService {

    private static final String AGGREGATE_TYPE = "INVENTORY";
    private static final String STATUS_RESERVED = "RESERVED";
    private static final String STATUS_FAILED = "FAILED";

    private final InventoryInboxEventRepository inboxRepository;
    private final InventoryReservationDecisionRepository decisionRepository;
    private final InventoryRepository inventoryRepository;
    private final EventStoreService eventStoreService;
    private final InventoryOutboxEventService outboxEventService;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final InventoryObservabilityMetrics metrics;

    @Transactional
    public InventoryProcessingResult process(EventEnvelope envelope) {
        validateEnvelope(envelope);
        OrderCreatedEvent order = readOrder(envelope);
        validateOrder(envelope, order);
        String payloadHash = hash(envelope);

        int inserted = inboxRepository.insertIfAbsent(
                envelope.eventId(),
                envelope.eventType(),
                envelope.aggregateId(),
                payloadHash,
                LocalDateTime.now(clock)
        );
        if (inserted == 0) {
            InventoryInboxEvent existing = inboxRepository.findById(envelope.eventId())
                    .orElseThrow(() -> new IllegalStateException("Inbox insert conflict did not return existing event"));
            if (!existing.getPayloadHash().equals(payloadHash)) {
                throw new InvalidInventoryMessageException(
                        "Duplicate eventId has different content: " + envelope.eventId());
            }
            metrics.inboxDuplicateAfterCommit(envelope.eventType());
            return InventoryProcessingResult.duplicateEvent(order.orderId());
        }

        try (MDC.MDCCloseable orderId = MDC.putCloseable("orderId", order.orderId());
             MDC.MDCCloseable eventId = MDC.putCloseable("eventId", envelope.eventId());
             MDC.MDCCloseable correlationId = MDC.putCloseable("correlationId", envelope.correlationId())) {
            decisionRepository.lockOrder(order.orderId());
            InventoryProcessingResult result = decisionRepository.findById(order.orderId())
                    .map(existing -> existingDecision(existing, order))
                    .orElseGet(() -> createDecision(envelope, order));
            log.info("Inventory decision completed | outcome: {}", result.outcome());
            return result;
        }
    }

    private InventoryProcessingResult existingDecision(InventoryReservationDecision existing,
                                                       OrderCreatedEvent order) {
        if (Objects.equals(existing.getProductId(), order.productId())
                && Objects.equals(existing.getQuantity(), order.quantity())) {
            return InventoryProcessingResult.existingDecision(order.orderId());
        }
        throw new OrderEventConflictException(
                "Order " + order.orderId() + " already has an inventory decision for different product or quantity");
    }

    private InventoryProcessingResult createDecision(EventEnvelope sourceEnvelope, OrderCreatedEvent order) {
        ReservationReply reply = reserve(order);
        String responseEventId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now(clock);
        EventEnvelope responseEnvelope = new EventEnvelope(
                responseEventId,
                reply.eventType(),
                order.orderId(),
                AGGREGATE_TYPE,
                now,
                EventSchemaVersions.V1,
                sourceEnvelope.correlationId(),
                reply.payload()
        );

        InventoryReservationDecision decision = InventoryReservationDecision.builder()
                .orderId(order.orderId())
                .sourceEventId(sourceEnvelope.eventId())
                .productId(order.productId())
                .quantity(order.quantity())
                .status(reply.success() ? STATUS_RESERVED : STATUS_FAILED)
                .failureReason(reply.success() ? null : reply.reason())
                .responseEventId(responseEventId)
                .decidedAt(now)
                .build();
        decisionRepository.save(decision);
        eventStoreService.saveEvent(order.orderId(), AGGREGATE_TYPE, reply.eventType(), responseEnvelope);
        outboxEventService.saveOutboxEvent(responseEventId, order.orderId(), AGGREGATE_TYPE,
                reply.eventType(), responseEnvelope);
        metrics.reservationResultAfterCommit(reply.success() ? "reserved" : "failed");
        return InventoryProcessingResult.created(order.orderId());
    }

    private ReservationReply reserve(OrderCreatedEvent order) {
        int updated = inventoryRepository.reserveIfAvailable(order.productId(), order.quantity());
        if (updated == 1) {
            InventoryReservedEvent payload = new InventoryReservedEvent(
                    order.orderId(),
                    order.productId(),
                    order.quantity(),
                    "Inventory reserved for: " + order.productId()
            );
            return new ReservationReply(EventTypes.INVENTORY_RESERVED, payload, true, null);
        }

        String reason = inventoryRepository.existsById(order.productId())
                ? "Insufficient inventory for: " + order.productId()
                : "Inventory item not found: " + order.productId();
        InventoryReservationFailedEvent payload = new InventoryReservationFailedEvent(
                order.orderId(),
                order.productId(),
                order.quantity(),
                reason
        );
        return new ReservationReply(EventTypes.INVENTORY_RESERVATION_FAILED, payload, false, reason);
    }

    private void validateEnvelope(EventEnvelope envelope) {
        if (envelope == null) {
            throw new InvalidInventoryMessageException("Event envelope is required");
        }
        requireText(envelope.eventId(), "eventId");
        requireText(envelope.aggregateId(), "aggregateId");
        if (!EventTypes.ORDER_CREATED.equals(envelope.eventType())) {
            throw new InvalidInventoryMessageException("Unsupported event type: " + envelope.eventType());
        }
        if (envelope.schemaVersion() != EventSchemaVersions.V1) {
            throw new InvalidInventoryMessageException("Unsupported schema version: " + envelope.schemaVersion());
        }
        if (envelope.payload() == null) {
            throw new InvalidInventoryMessageException("payload is required");
        }
    }

    private OrderCreatedEvent readOrder(EventEnvelope envelope) {
        try {
            return objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class);
        } catch (IllegalArgumentException ex) {
            throw new InvalidInventoryMessageException("Invalid ORDER_CREATED payload", ex);
        }
    }

    private void validateOrder(EventEnvelope envelope, OrderCreatedEvent order) {
        requireText(order.orderId(), "payload.orderId");
        requireText(order.productId(), "payload.productId");
        if (!envelope.aggregateId().equals(order.orderId())) {
            throw new InvalidInventoryMessageException("aggregateId must match payload.orderId");
        }
        if (order.quantity() == null || order.quantity() <= 0) {
            throw new InvalidInventoryMessageException("payload.quantity must be a positive integer");
        }
    }

    private String hash(EventEnvelope envelope) {
        try {
            byte[] json = objectMapper.writeValueAsBytes(envelope);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(json));
        } catch (JsonProcessingException ex) {
            throw new InvalidInventoryMessageException("Event envelope cannot be serialized for inbox hashing", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 digest is unavailable", ex);
        }
    }

    private void requireText(String value, String field) {
        if (!StringUtils.hasText(value)) {
            throw new InvalidInventoryMessageException(field + " is required");
        }
    }

    private record ReservationReply(String eventType, Object payload, boolean success, String reason) {
    }
}
