package com.example.orderservice.outbox;

import com.example.common.event.EventEnvelope;
import com.example.common.event.EventTypes;
import com.example.common.event.OrderCreatedEvent;
import com.example.orderservice.entity.OutboxEvent;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaOperations<String, Object> kafkaTemplate;

    @InjectMocks
    private OutboxRelay outboxRelay;

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void publishesEnvelopeFromOutboxPayload() throws Exception {
        ReflectionTestUtils.setField(outboxRelay, "ordersTopic", "orders");
        ReflectionTestUtils.setField(outboxRelay, "objectMapper", objectMapper);

        EventEnvelope envelope = new EventEnvelope(
                "event-1",
                EventTypes.ORDER_CREATED,
                "order-1",
                "ORDER",
                LocalDateTime.now(),
                1,
                "order-1",
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
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id("outbox-1")
                .aggregateId("order-1")
                .eventType(EventTypes.ORDER_CREATED)
                .payload(objectMapper.writeValueAsString(envelope))
                .status("PENDING")
                .build();

        when(outboxEventRepository.findByStatus("PENDING")).thenReturn(List.of(outboxEvent));
        when(kafkaTemplate.send(eq("orders"), eq("order-1"), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        outboxRelay.processOutboxEvents();

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq("orders"), eq("order-1"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(captor.getValue().aggregateId()).isEqualTo("order-1");
        assertThat(outboxEvent.getStatus()).isEqualTo("SENT");
    }
}
