package com.example.replay;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaDltReplayToolTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withStartupTimeout(Duration.ofSeconds(120));

    @BeforeAll
    static void createTopics() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(
                    new NewTopic("orders", 1, (short) 1),
                    new NewTopic("orders.DLT", 1, (short) 1)
            )).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void dryRunDoesNotPublishAndExecuteReplaysOnlySelectedRecord() throws Exception {
        byte[] payload = """
                {"eventId":"event-1","eventType":"ORDER_CREATED","aggregateId":"order-1","aggregateType":"ORDER","occurredAt":"2026-09-24T12:00:00","schemaVersion":1,"correlationId":"order-1","payload":{"orderId":"order-1","productId":"product-1","customerId":"customer-1","quantity":2,"price":12.50,"status":"PENDING","createdAt":"2026-09-24T12:00:00"}}
                """.getBytes(StandardCharsets.UTF_8);
        long selectedOffset = publishDlt("order-1", payload);
        publishDlt("order-ignored", "{\"eventId\":\"ignored\"}".getBytes(StandardCharsets.UTF_8));
        var config = Files.createTempFile("replay", ".properties");
        Files.writeString(config, "replay.destination.orders.DLT=orders%n".formatted());

        ByteArrayOutputStream dryRunOutput = new ByteArrayOutputStream();
        int dryRunExit = new KafkaDltReplayTool(new PrintStream(dryRunOutput), System.err).run(new String[]{
                "--bootstrap-servers", KAFKA.getBootstrapServers(),
                "--dlt-topic", "orders.DLT",
                "--partition", "0",
                "--offset", Long.toString(selectedOffset),
                "--config", config.toString(),
                "--timeout-ms", "10000"
        });

        assertThat(dryRunExit).isZero();
        assertThat(dryRunOutput.toString(StandardCharsets.UTF_8)).contains("Dry-run only");
        assertThat(readOne("orders", "order-1", Duration.ofMillis(500))).isNull();

        ByteArrayOutputStream executeOutput = new ByteArrayOutputStream();
        int executeExit = new KafkaDltReplayTool(new PrintStream(executeOutput), System.err).run(new String[]{
                "--bootstrap-servers", KAFKA.getBootstrapServers(),
                "--dlt-topic", "orders.DLT",
                "--partition", "0",
                "--offset", Long.toString(selectedOffset),
                "--config", config.toString(),
                "--timeout-ms", "10000",
                "--execute"
        });

        assertThat(executeExit).isZero();
        ConsumerRecord<String, byte[]> replayed = readOne("orders", "order-1", Duration.ofSeconds(10));
        assertThat(replayed).isNotNull();
        assertThat(replayed.key()).isEqualTo("order-1");
        assertThat(replayed.value()).isEqualTo(payload);
        assertThat(replayed.headers().lastHeader("x-replay-id")).isNotNull();
        assertThat(replayed.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNull();
        assertThat(executeOutput.toString(StandardCharsets.UTF_8)).contains("Replay published");
    }

    @Test
    void invalidDestinationConfigFailsWithoutSending() throws Exception {
        long selectedOffset = publishDlt("order-no-dest", "{}".getBytes(StandardCharsets.UTF_8));
        var config = Files.createTempFile("replay-empty", ".properties");
        Files.writeString(config, "");

        int exit = new KafkaDltReplayTool(System.out, System.err).run(new String[]{
                "--bootstrap-servers", KAFKA.getBootstrapServers(),
                "--dlt-topic", "orders.DLT",
                "--partition", "0",
                "--offset", Long.toString(selectedOffset),
                "--config", config.toString(),
                "--timeout-ms", "10000",
                "--execute"
        });

        assertThat(exit).isEqualTo(5);
        assertThat(readOne("orders", "order-no-dest", Duration.ofMillis(500))).isNull();
    }

    private long publishDlt(String key, byte[] payload) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        RecordHeaders headers = new RecordHeaders();
        headers.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders".getBytes(StandardCharsets.UTF_8));
        headers.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, "0".getBytes(StandardCharsets.UTF_8));
        headers.add(KafkaHeaders.DLT_ORIGINAL_OFFSET, "12".getBytes(StandardCharsets.UTF_8));
        headers.add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, "inventory-group".getBytes(StandardCharsets.UTF_8));
        headers.add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, "bad json".getBytes(StandardCharsets.UTF_8));
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            return producer.send(new ProducerRecord<>("orders.DLT", null, key, payload, headers))
                    .get(30, TimeUnit.SECONDS).offset();
        }
    }

    private ConsumerRecord<String, byte[]> readOne(String topic, String key, Duration timeout) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
            return null;
        }
    }
}
