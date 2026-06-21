package com.example.inventoryservice.consumer;

import com.example.common.event.EventEnvelope;
import com.example.inventoryservice.service.InventoryProcessingResult;
import com.example.inventoryservice.service.InventoryReservationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryConsumerTest {

    @Test
    void delegatesOrderEnvelopeToReservationService() {
        CapturingReservationService reservationService = new CapturingReservationService();
        InventoryConsumer inventoryConsumer = new InventoryConsumer(reservationService);
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
    }

    private static class CapturingReservationService extends InventoryReservationService {
        private EventEnvelope envelope;

        CapturingReservationService() {
            super(null, null, null, null, null, null, null);
        }

        @Override
        public InventoryProcessingResult process(EventEnvelope envelope) {
            this.envelope = envelope;
            return InventoryProcessingResult.created("order-1");
        }
    }
}
