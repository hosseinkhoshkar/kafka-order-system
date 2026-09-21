package com.example.inventoryservice.observability;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = HttpServerMetricsPrometheusTest.TestApp.class,
        properties = {
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration"
        })
@AutoConfigureObservability
class HttpServerMetricsPrometheusTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void prometheusPublishesHttpServerHistogramBuckets() {
        assertThat(restTemplate.getForEntity("http://localhost:" + port + "/test-http-metrics", String.class)
                .getStatusCode().is2xxSuccessful()).isTrue();

        String prometheus = restTemplate.getForObject("http://localhost:" + port + "/actuator/prometheus", String.class);

        assertThat(prometheus).contains("http_server_requests_seconds_bucket{");
        assertThat(prometheus).contains("http_server_requests_seconds_count{");
        assertThat(prometheus).contains("http_server_requests_seconds_sum{");
        assertThat(Pattern.compile("http_server_requests_seconds_bucket\\{[^}]*uri=\"/test-http-metrics\"[^}]*le=\"5\\.0\"[^}]*}\\s+[1-9]")
                .matcher(prometheus).find()).isTrue();
        assertThat(Pattern.compile("http_server_requests_seconds_count\\{[^}]*uri=\"/test-http-metrics\"[^}]*}\\s+[1-9]")
                .matcher(prometheus).find()).isTrue();
        assertThat(Pattern.compile("http_server_requests_seconds_sum\\{[^}]*uri=\"/test-http-metrics\"[^}]*}\\s+[0-9]")
                .matcher(prometheus).find()).isTrue();
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @RestController
    static class TestApp {
        static void main(String[] args) {
            SpringApplication.run(TestApp.class, args);
        }

        @GetMapping("/test-http-metrics")
        String testHttpMetrics() {
            return "ok";
        }

        @Bean
        HealthIndicator dbHealthIndicator() {
            return () -> Health.up().build();
        }

        @Bean
        HealthIndicator kafkaHealthIndicator() {
            return () -> Health.up().build();
        }
    }
}
