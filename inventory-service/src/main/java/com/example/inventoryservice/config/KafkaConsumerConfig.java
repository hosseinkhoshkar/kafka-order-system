package com.example.inventoryservice.config;

import com.example.common.event.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.listener.auto-startup:true}")
    private boolean autoStartup;
    @Value("${kafka.consumer.group.inventory}")
    private String inventoryGroup;
    @Value("${kafka.topic.orders}")
    private String ordersTopic;
    @Value("${kafka.topic.orders-dlt}")
    private String ordersDltTopic;
    @Value("${kafka.consumer.error.max-attempts:3}")
    private int maxAttempts;
    @Value("${kafka.consumer.error.backoff-ms:1000}")
    private long backoffMs;
    @Value("${kafka.consumer.error.dlt-send-timeout:PT10S}")
    private Duration dltSendTimeout;

    @Bean
    public ConsumerFactory<String, EventEnvelope> consumerFactory(ObjectMapper objectMapper) {
        JsonDeserializer<EventEnvelope> delegate = new JsonDeserializer<>(EventEnvelope.class, objectMapper, false);
        delegate.setUseTypeHeaders(false);
        delegate.setRemoveTypeHeaders(false);
        delegate.addTrustedPackages("*");

        Map<String, Object> config = consumerConfig(inventoryGroup, ErrorHandlingDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(delegate));
    }

    @Bean
    public ConsumerFactory<String, byte[]> byteArrayConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(inventoryGroup + "-raw", ByteArrayDeserializer.class),
                new StringDeserializer(),
                new ByteArrayDeserializer());
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate, Clock clock) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(dltTopic(record.topic()), record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(dltSendTimeout);
        recoverer.setAppendOriginalHeaders(true);
        recoverer.setStripPreviousExceptionHeaders(true);
        recoverer.setRetainExceptionHeader(true);
        recoverer.excludeHeader(HeadersToAdd.EX_STACKTRACE);
        recoverer.addHeadersFunction((record, exception) -> {
            RecordHeaders headers = new RecordHeaders();
            headers.add("x-dlt-failed-at", Instant.now(clock).toString().getBytes(StandardCharsets.UTF_8));
            headers.add("x-dlt-error-class", rootClassName(exception).getBytes(StandardCharsets.UTF_8));
            return headers;
        });

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(backoffMs, Math.max(maxAttempts - 1L, 0L)));
        errorHandler.addNotRetryableExceptions(
                com.example.inventoryservice.exception.InvalidInventoryMessageException.class,
                com.example.inventoryservice.exception.OrderEventConflictException.class,
                org.springframework.kafka.support.serializer.DeserializationException.class,
                IllegalArgumentException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> kafkaListenerContainerFactory(
            ConsumerFactory<String, EventEnvelope> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setAutoStartup(autoStartup);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> byteArrayKafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> byteArrayConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(byteArrayConsumerFactory);
        factory.setAutoStartup(autoStartup);
        return factory;
    }

    private Map<String, Object> consumerConfig(String groupId, Class<?> valueDeserializer) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return config;
    }

    private String dltTopic(String sourceTopic) {
        return ordersTopic.equals(sourceTopic) ? ordersDltTopic : sourceTopic + ".DLT";
    }

    private String rootClassName(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getClass().getName();
    }
}
