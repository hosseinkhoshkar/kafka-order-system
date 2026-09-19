package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.inventoryservice.observability.InventoryObservabilityMetrics;
import com.example.inventoryservice.service.InventoryProcessingResult;
import com.example.inventoryservice.service.InventoryReservationService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryConsumerTest {

    @Test
    void delegatesOrderEnvelopeToReservationService() {
        CapturingReservationService reservationService = new CapturingReservationService();
        InventoryConsumer inventoryConsumer = new InventoryConsumer(
                reservationService,
                new InventoryObservabilityMetrics(new SimpleMeterRegistry()));
        EventEnvelope envelope = new EventEnvelope(
                "event-1",
                "ORDER_CREATED",
                "order-1",
                "ORDER",
                LocalDateTime.now(),
                1,
                "correlation-1",
                new Object()
        );

        inventoryConsumer.consumeOrder(new ConsumerRecord<>("orders", 0, 0, "order-1", envelope));

        assertThat(reservationService.envelope).isSameAs(envelope);
        assertThat(MDC.get("orderId")).isNull();
        assertThat(MDC.get("eventId")).isNull();
        assertThat(MDC.get("correlationId")).isNull();
    }

    private static class CapturingReservationService extends InventoryReservationService {
        private EventEnvelope envelope;

        CapturingReservationService() {
            super(null, null, null, null, null, null, null,
                    new InventoryObservabilityMetrics(new SimpleMeterRegistry()));
        }

        @Override
        public InventoryProcessingResult process(EventEnvelope envelope) {
            this.envelope = envelope;
            return InventoryProcessingResult.created("order-1");
        }
    }
}
