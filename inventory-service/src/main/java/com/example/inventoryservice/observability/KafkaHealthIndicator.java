package com.example.inventoryservice.observability;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final String bootstrapServers;
    private final Duration timeout;

    public KafkaHealthIndicator(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                @Value("${observability.kafka.health-timeout-ms:1000}") long timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    @Override
    public Health health() {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis(),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis()
        );
        try (AdminClient adminClient = AdminClient.create(config)) {
            String clusterId = adminClient.describeCluster()
                    .clusterId()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return Health.up().withDetail("clusterId", clusterId).build();
        } catch (Exception ex) {
            return Health.down(ex).build();
        }
    }
}
