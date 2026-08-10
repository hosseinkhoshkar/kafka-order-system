package com.example.common.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventContractCompatibilityTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .build();

    @Test
    void v1FixturesRemainReadable() throws Exception {
        EventEnvelope order = read("order-created-v1.json");
        OrderCreatedEvent orderPayload = objectMapper.convertValue(order.payload(), OrderCreatedEvent.class);
        assertThat(order.schemaVersion()).isEqualTo(EventSchemaVersions.V1);
        assertThat(order.eventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(orderPayload.price()).isEqualByComparingTo(new BigDecimal("12.50"));

        EventEnvelope reserved = read("inventory-reserved-v1.json");
        InventoryReservedEvent reservedPayload = objectMapper.convertValue(
                reserved.payload(), InventoryReservedEvent.class);
        assertThat(reservedPayload.orderId()).isEqualTo("order-1");

        EventEnvelope failed = read("inventory-reservation-failed-v1.json");
        InventoryReservationFailedEvent failedPayload = objectMapper.convertValue(
                failed.payload(), InventoryReservationFailedEvent.class);
        assertThat(failedPayload.reason()).contains("Insufficient inventory");
    }

    @Test
    void unknownOptionalPayloadFieldsAreCompatible() throws Exception {
        JsonNode root = objectMapper.readTree(resource("order-created-v1.json"));
        var mutable = (com.fasterxml.jackson.databind.node.ObjectNode) root.get("payload");
        mutable.put("optionalComment", "new optional data");

        EventEnvelope envelope = objectMapper.treeToValue(root, EventEnvelope.class);
        OrderCreatedEvent payload = objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class);

        assertThat(payload.orderId()).isEqualTo("order-1");
        assertThat(payload.quantity()).isEqualTo(2);
    }

    @Test
    void incompatibleRequiredFieldTypeFailsClearly() throws Exception {
        JsonNode root = objectMapper.readTree(resource("order-created-v1.json"));
        var mutable = (com.fasterxml.jackson.databind.node.ObjectNode) root.get("payload");
        mutable.put("quantity", "two");

        EventEnvelope envelope = objectMapper.treeToValue(root, EventEnvelope.class);

        assertThatThrownBy(() -> objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private EventEnvelope read(String name) throws Exception {
        return objectMapper.readValue(resource(name), EventEnvelope.class);
    }

    private byte[] resource(String name) throws Exception {
        try (var input = getClass().getResourceAsStream("/contracts/" + name)) {
            return input.readAllBytes();
        }
    }
}
