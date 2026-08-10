package com.example.replay;

import com.example.common.event.EventEnvelope;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.SerializationUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaDltReplayTool {

    private static final int OK = 0;
    private static final int USAGE = 2;
    private static final int NOT_FOUND = 3;
    private static final int SEND_FAILED = 4;
    private static final int CONFIG_ERROR = 5;

    private static final Set<String> CONTROL_HEADERS = Set.of(
            KafkaHeaders.DLT_EXCEPTION_FQCN,
            KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN,
            KafkaHeaders.DLT_EXCEPTION_STACKTRACE,
            KafkaHeaders.DLT_EXCEPTION_MESSAGE,
            KafkaHeaders.DLT_KEY_EXCEPTION_FQCN,
            KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE,
            KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE,
            KafkaHeaders.DLT_ORIGINAL_TOPIC,
            KafkaHeaders.DLT_ORIGINAL_PARTITION,
            KafkaHeaders.DLT_ORIGINAL_OFFSET,
            KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP,
            KafkaHeaders.DLT_ORIGINAL_TIMESTAMP,
            KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE,
            KafkaHeaders.DELIVERY_ATTEMPT,
            KafkaHeaders.EXCEPTION_FQCN,
            KafkaHeaders.EXCEPTION_CAUSE_FQCN,
            KafkaHeaders.EXCEPTION_STACKTRACE,
            KafkaHeaders.EXCEPTION_MESSAGE,
            SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER,
            SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
            "x-dlt-failed-at",
            "x-dlt-error-class"
    );

    private final PrintStream out;
    private final PrintStream err;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public KafkaDltReplayTool(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        int exitCode = new KafkaDltReplayTool(System.out, System.err).run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public int run(String[] args) {
        Arguments arguments;
        try {
            arguments = Arguments.parse(args);
        } catch (IllegalArgumentException ex) {
            err.println(ex.getMessage());
            printUsage(err);
            return USAGE;
        }

        Properties config;
        try {
            config = loadReplayConfig(arguments.configPath);
        } catch (IOException ex) {
            err.println("Cannot read replay config: " + ex.getMessage());
            return CONFIG_ERROR;
        }

        String destination = config.getProperty("replay.destination." + arguments.dltTopic);
        if (destination == null || destination.isBlank()) {
            err.println("No trusted replay destination configured for DLT topic " + arguments.dltTopic);
            return CONFIG_ERROR;
        }

        TopicPartition source = new TopicPartition(arguments.dltTopic, arguments.partition);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties(arguments))) {
            ConsumerRecord<String, byte[]> record = readRecord(consumer, source, arguments.offset, arguments.timeout);
            if (record == null) {
                err.printf("No DLT record found at %s-%d offset %d%n",
                        arguments.dltTopic, arguments.partition, arguments.offset);
                return NOT_FOUND;
            }

            printSummary(record, destination, arguments.execute);
            if (!arguments.execute) {
                out.println("Dry-run only: no message was published and no offset was committed.");
                return OK;
            }

            String replayId = UUID.randomUUID().toString();
            Instant replayedAt = Instant.now();
            Headers headers = replayHeaders(record, replayId, replayedAt);
            ProducerRecord<String, byte[]> replay = new ProducerRecord<>(
                    destination, null, record.key(), record.value(), headers);
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties(arguments))) {
                var metadata = producer.send(replay).get(arguments.timeout.toMillis(), TimeUnit.MILLISECONDS);
                producer.flush();
                out.printf("Replay published | replayId=%s | replayedAt=%s | destination=%s-%d offset=%d | source=%s-%d offset=%d%n",
                        replayId, replayedAt, metadata.topic(), metadata.partition(), metadata.offset(),
                        record.topic(), record.partition(), record.offset());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                err.println("Replay interrupted before broker acknowledgement; send outcome is unknown.");
                return SEND_FAILED;
            } catch (ExecutionException | TimeoutException ex) {
                err.println("Replay send failed or timed out before broker acknowledgement; send outcome is unknown: "
                        + sanitize(ex.getMessage()));
                return SEND_FAILED;
            }
            return OK;
        }
    }

    private ConsumerRecord<String, byte[]> readRecord(KafkaConsumer<String, byte[]> consumer,
                                                      TopicPartition source,
                                                      long offset,
                                                      Duration timeout) {
        consumer.assign(java.util.List.of(source));
        long beginning = consumer.beginningOffsets(java.util.List.of(source)).get(source);
        long end = consumer.endOffsets(java.util.List.of(source)).get(source);
        if (offset < beginning || offset >= end) {
            return null;
        }
        consumer.seek(source, offset);
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                if (record.partition() == source.partition() && record.offset() == offset) {
                    return record;
                }
            }
        }
        return null;
    }

    private void printSummary(ConsumerRecord<String, byte[]> record, String destination, boolean execute) {
        out.printf("Mode: %s%n", execute ? "execute" : "dry-run");
        out.printf("Source DLT: %s-%d offset %d%n", record.topic(), record.partition(), record.offset());
        out.printf("Proposed destination: %s%n", destination);
        out.printf("Key: %s%n", record.key());
        out.printf("Payload bytes: %d%n", record.value() == null ? 0 : record.value().length);
        out.printf("Original topic: %s%n", header(record.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC));
        out.printf("Original partition: %s%n", header(record.headers(), KafkaHeaders.DLT_ORIGINAL_PARTITION));
        out.printf("Original offset: %s%n", header(record.headers(), KafkaHeaders.DLT_ORIGINAL_OFFSET));
        out.printf("Original group: %s%n", header(record.headers(), KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP));
        out.printf("Failure class: %s%n", firstPresent(
                header(record.headers(), "x-dlt-error-class"),
                header(record.headers(), KafkaHeaders.DLT_EXCEPTION_FQCN)));
        out.printf("Failure message: %s%n", sanitize(header(record.headers(), KafkaHeaders.DLT_EXCEPTION_MESSAGE)));

        if (record.value() != null) {
            try {
                EventEnvelope envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
                out.printf("Envelope: eventId=%s eventType=%s aggregateId=%s schemaVersion=%d correlationId=%s%n",
                        envelope.eventId(), envelope.eventType(), envelope.aggregateId(),
                        envelope.schemaVersion(), envelope.correlationId());
                if (envelope.schemaVersion() != 1) {
                    out.println("Warning: unsupported schemaVersion. Replay is not a fix unless the consumer now supports it.");
                }
            } catch (Exception ex) {
                out.println("Warning: payload cannot be read as EventEnvelope. Replay is not a fix until the payload/schema issue is corrected.");
            }
        } else {
            out.println("Warning: null/tombstone value. Replay will republish the null value only if --execute is used.");
        }
    }

    private Headers replayHeaders(ConsumerRecord<String, byte[]> record, String replayId, Instant replayedAt) {
        RecordHeaders headers = new RecordHeaders();
        for (Header header : record.headers()) {
            if (!isControlHeader(header.key())) {
                headers.add(header);
            }
        }
        headers.add("x-replay-id", replayId.getBytes(StandardCharsets.UTF_8));
        headers.add("x-replayed-at", replayedAt.toString().getBytes(StandardCharsets.UTF_8));
        headers.add("x-replay-source-topic", record.topic().getBytes(StandardCharsets.UTF_8));
        headers.add("x-replay-source-partition", Integer.toString(record.partition()).getBytes(StandardCharsets.UTF_8));
        headers.add("x-replay-source-offset", Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private boolean isControlHeader(String name) {
        return CONTROL_HEADERS.contains(name)
                || name.startsWith("kafka_dlt-")
                || name.startsWith("springDeserializerException");
    }

    private static Properties consumerProperties(Arguments arguments) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, arguments.bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-dlt-replay-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    private static Properties producerProperties(Arguments arguments) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, arguments.bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Long.toString(arguments.timeout.toMillis()));
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(Math.max(1000, arguments.timeout.toMillis() / 2)));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    private static Properties loadReplayConfig(Path path) throws IOException {
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return properties;
    }

    private static String header(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return "";
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String firstPresent(String first, String second) {
        return first == null || first.isBlank() ? second : first;
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "";
        }
        String sanitized = value.replaceAll("\\s+", " ").trim();
        return sanitized.length() <= 512 ? sanitized : sanitized.substring(0, 512);
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: java -jar kafka-replay-tool.jar --bootstrap-servers host:9092 --dlt-topic orders.DLT --partition 0 --offset 42 [--execute] [--config ops/replay.properties] [--timeout-ms 10000]");
    }

    private record Arguments(String bootstrapServers,
                             String dltTopic,
                             int partition,
                             long offset,
                             boolean execute,
                             Path configPath,
                             Duration timeout) {

        static Arguments parse(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            boolean execute = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if ("--execute".equals(arg)) {
                    execute = true;
                    continue;
                }
                if (!arg.startsWith("--") || i + 1 >= args.length) {
                    throw new IllegalArgumentException("Invalid argument list: " + Arrays.toString(args));
                }
                values.put(arg.substring(2), args[++i]);
            }
            String bootstrapServers = required(values, "bootstrap-servers");
            String dltTopic = required(values, "dlt-topic");
            int partition = Integer.parseInt(required(values, "partition"));
            long offset = Long.parseLong(required(values, "offset"));
            Path configPath = Path.of(values.getOrDefault("config", "ops/replay.properties"));
            long timeoutMs = Long.parseLong(values.getOrDefault("timeout-ms", "10000"));
            if (partition < 0 || offset < 0 || timeoutMs <= 0) {
                throw new IllegalArgumentException("partition, offset and timeout-ms must be non-negative/positive values");
            }
            return new Arguments(bootstrapServers, dltTopic, partition, offset, execute, configPath,
                    Duration.ofMillis(timeoutMs));
        }

        private static String required(Map<String, String> values, String key) {
            String value = values.get(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing required argument --" + key);
            }
            return value;
        }
    }
}
