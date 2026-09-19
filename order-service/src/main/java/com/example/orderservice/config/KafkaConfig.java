package com.example.orderservice.config;

import com.example.common.event.EventEnvelope;
import com.example.orderservice.model.Order;
import com.example.orderservice.observability.OrderObservabilityMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.consumer.group.order}")
    private String orderGroup;
    @Value("${kafka.topic.inventory-reply}")
    private String inventoryReplyTopic;
    @Value("${kafka.topic.inventory-reply-dlt}")
    private String inventoryReplyDltTopic;
    @Value("${kafka.consumer.error.max-attempts:3}")
    private int maxAttempts;
    @Value("${kafka.consumer.error.backoff-ms:1000}")
    private long backoffMs;
    @Value("${kafka.consumer.error.dlt-send-timeout:PT10S}")
    private Duration dltSendTimeout;
    @Value("${spring.kafka.listener.auto-startup:true}")
    private boolean autoStartup;

    @Bean
    public ProducerFactory<String, Order> producerFactory() {
        Map<String, Object> config = producerConfig(JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> objectProducerFactory() {
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new JsonSerializer<>());

        Map<String, Object> config = producerConfig(DelegatingByTypeSerializer.class);
        return new DefaultKafkaProducerFactory<>(
                config,
                new StringSerializer(),
                new DelegatingByTypeSerializer(delegates, true));
    }

    @Bean
    public KafkaTemplate<String, Object> objectKafkaTemplate(OrderObservabilityMetrics metrics) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(objectProducerFactory());
        template.setProducerListener(dltProducerListener(metrics));
        return template;
    }

    @Bean
    public ConsumerFactory<String, EventEnvelope> consumerFactory(ObjectMapper objectMapper) {
        JsonDeserializer<EventEnvelope> delegate = new JsonDeserializer<>(EventEnvelope.class, objectMapper, false);
        delegate.addTrustedPackages("*");
        delegate.setUseTypeHeaders(false);
        delegate.setRemoveTypeHeaders(false);

        Map<String, Object> config = consumerConfig(orderGroup, ErrorHandlingDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(delegate));
    }

    @Bean
    public ConsumerFactory<String, byte[]> byteArrayConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(orderGroup + "-raw", ByteArrayDeserializer.class),
                new StringDeserializer(),
                new ByteArrayDeserializer());
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> objectKafkaTemplate, Clock clock) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                objectKafkaTemplate,
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

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(backoffMs, Math.max(maxAttempts - 1L, 0L)));
        handler.addNotRetryableExceptions(
                com.example.orderservice.exception.InvalidInventoryReplyException.class,
                com.example.orderservice.exception.InventoryReplyConflictException.class,
                org.springframework.kafka.support.serializer.DeserializationException.class,
                IllegalArgumentException.class);
        return handler;
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

    private Map<String, Object> producerConfig(Class<?> valueSerializer) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return config;
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
        return inventoryReplyTopic.equals(sourceTopic) ? inventoryReplyDltTopic : sourceTopic + ".DLT";
    }

    private String rootClassName(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getClass().getName();
    }

    private ProducerListener<String, Object> dltProducerListener(OrderObservabilityMetrics metrics) {
        return new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<String, Object> producerRecord, RecordMetadata recordMetadata) {
                if (producerRecord.topic().endsWith(".DLT")) {
                    metrics.dltProducerResult(producerRecord.topic(), "success");
                }
            }

            @Override
            public void onError(ProducerRecord<String, Object> producerRecord,
                                RecordMetadata recordMetadata,
                                Exception exception) {
                if (producerRecord.topic().endsWith(".DLT")) {
                    metrics.dltProducerResult(producerRecord.topic(), "failure");
                }
            }
        };
    }
}
