package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.inventoryservice.service.EventStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryConsumerTest {

    @Mock
    private KafkaOperations<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void consumesOrderCreatedEnvelopeAndPublishesReservedEnvelope() {
        CapturingEventStoreService eventStoreService = new CapturingEventStoreService(objectMapper);
        InventoryConsumer inventoryConsumer = new InventoryConsumer(kafkaTemplate, eventStoreService, objectMapper);
        ReflectionTestUtils.setField(inventoryConsumer, "inventoryReplyTopic", "inventory-reply");

        EventEnvelope envelope = new EventEnvelope(
                "event-1",
                EventTypes.ORDER_CREATED,
                "order-1",
                "ORDER",
                LocalDateTime.now(),
                1,
                "correlation-1",
                new OrderCreatedEvent(
                        "order-1",
                        "product-1",
                        "customer-1",
                        2,
                        BigDecimal.TEN,
                        "PENDING",
                        LocalDateTime.now()
                )
        );
        ConsumerRecord<String, EventEnvelope> record = new ConsumerRecord<>("orders", 0, 0, "order-1", envelope);

        when(kafkaTemplate.send(eq("inventory-reply"), eq("order-1"), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        inventoryConsumer.consumeOrder(record);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq("inventory-reply"), eq("order-1"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo(EventTypes.INVENTORY_RESERVED);
        assertThat(captor.getValue().aggregateId()).isEqualTo("order-1");
        assertThat(captor.getValue().correlationId()).isEqualTo("correlation-1");
        assertThat(eventStoreService.eventType).isEqualTo(EventTypes.INVENTORY_RESERVED);
    }

    private static class CapturingEventStoreService extends EventStoreService {
        private String eventType;

        CapturingEventStoreService(ObjectMapper objectMapper) {
            super(null, objectMapper);
        }

        @Override
        public void saveEvent(String aggregateId, String aggregateType, String eventType, Object payload) {
            this.eventType = eventType;
        }
    }
}
