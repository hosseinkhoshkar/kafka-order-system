package com.example.system;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Exercises packaged applications through HTTP, Kafka and independent databases. */
@Testcontainers
class OrderFlowIT {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @Container
    static final PostgreSQLContainer<?> ORDER_DB = new PostgreSQLContainer<>("postgres:15.6")
            .withDatabaseName("orderdb");
    @Container
    static final PostgreSQLContainer<?> INVENTORY_DB = new PostgreSQLContainer<>("postgres:15.6")
            .withDatabaseName("inventorydb");

    private static final Path ROOT = Path.of(System.getProperty("repositoryRoot")).toAbsolutePath().normalize();
    private static final Path LOGS = ROOT.resolve("integration-tests/target/application-logs");
    private static final List<Process> APPLICATIONS = new ArrayList<>();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static int orderPort;

    @BeforeAll
    static void startApplications() throws Exception {
        Files.createDirectories(LOGS);
        try {
            start("inventory-service", INVENTORY_DB);
            start("order-service", ORDER_DB);
            awaitPort("inventory-service", APPLICATIONS.get(0));
            orderPort = awaitPort("order-service", APPLICATIONS.get(1));
            HttpResponse<String> health = HTTP.send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + orderPort + "/api/orders/health"))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(health.body()).isEqualTo("Order Service is running");
        } catch (Exception | AssertionError failure) {
            stopApplications();
            throw failure;
        }
    }

    private static void start(String service, PostgreSQLContainer<?> database) throws Exception {
        Path jar = ROOT.resolve(service + "/target/" + service + "-0.0.1-SNAPSHOT.jar");
        assertThat(jar).as("Run the root Maven reactor with verify to package both services").exists();
        List<String> command = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-jar", jar.toString(),
                "--server.port=0",
                "--spring.datasource.url=" + database.getJdbcUrl(),
                "--spring.datasource.username=" + database.getUsername(),
                "--spring.datasource.password=" + database.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.jpa.hibernate.ddl-auto=create-drop",
                "--spring.jpa.show-sql=false",
                "--spring.main.banner-mode=off");
        APPLICATIONS.add(new ProcessBuilder(command).directory(ROOT.toFile())
                .redirectErrorStream(true).redirectOutput(LOGS.resolve(service + ".log").toFile()).start());
    }

    private static int awaitPort(String service, Process application) {
        Pattern port = Pattern.compile("Tomcat started on port (\\d+)");
        int[] result = {0};
        await().alias(service + " startup; see " + LOGS).atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(500)).until(() -> {
                    if (!application.isAlive()) {
                        throw new IllegalStateException(service + " exited: "
                                + Files.readString(LOGS.resolve(service + ".log")));
                    }
                    var matcher = port.matcher(Files.readString(LOGS.resolve(service + ".log")));
                    if (!matcher.find()) return false;
                    result[0] = Integer.parseInt(matcher.group(1));
                    return true;
                });
        return result[0];
    }

    @ParameterizedTest(name = "quantity {0} eventually becomes {1}")
    @CsvSource({
            "2, CONFIRMED, INVENTORY_RESERVED, ORDER_CONFIRMED",
            "11, CANCELLED, INVENTORY_RESERVATION_FAILED, ORDER_CANCELLED"
    })
    void orderTraversesOutboxKafkaInventoryAndReply(int quantity, String status,
                                                   String inventoryEvent, String orderEvent) throws Exception {
        String product = "product-" + UUID.randomUUID();
        String body = JSON.writeValueAsString(Map.of("productId", product,
                "customerId", "customer-test", "quantity", quantity, "price", 12.5));
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + orderPort + "/api/orders"))
                .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        JsonNode order = JSON.readTree(response.body());
        assertThat(order.path("status").asText()).isEqualTo("PENDING");
        assertThat(order.path("quantity").asInt()).isEqualTo(quantity);
        String orderId = order.path("orderId").asText();
        assertThat(orderId).isNotBlank();

        await().alias("Order " + orderId + " reaches " + status + "; see " + LOGS)
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
                    assertThat(scalar(ORDER_DB, "select status from orders where order_id = ?", orderId))
                            .isEqualTo(status);
                    assertThat(scalar(ORDER_DB,
                            "select status from outbox_events where aggregate_id = ?", orderId)).isEqualTo("SENT");
                    assertThat(scalar(ORDER_DB,
                            "select count(*) from event_store where aggregate_id = ? and event_type = ?",
                            orderId, "ORDER_CREATED")).isEqualTo("1");
                    assertThat(scalar(ORDER_DB,
                            "select count(*) from event_store where aggregate_id = ? and event_type = ?",
                            orderId, orderEvent)).isEqualTo("1");
                    assertThat(scalar(INVENTORY_DB,
                            "select count(*) from event_store where aggregate_id = ? and event_type = ?",
                            orderId, inventoryEvent)).isEqualTo("1");
                });
        JsonNode envelope = JSON.readTree(scalar(INVENTORY_DB,
                "select payload from event_store where aggregate_id = ? and event_type = ?", orderId, inventoryEvent));
        assertThat(envelope.path("correlationId").asText()).isEqualTo(orderId);
        assertThat(envelope.path("payload").path("productId").asText()).isEqualTo(product);
    }

    private static String scalar(PostgreSQLContainer<?> database, String sql, String... parameters) throws Exception {
        try (var connection = DriverManager.getConnection(database.getJdbcUrl(),
                database.getUsername(), database.getPassword());
             var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) statement.setString(i + 1, parameters[i]);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    @AfterAll
    static void stopApplications() throws InterruptedException {
        for (Process application : APPLICATIONS) application.destroy();
        for (Process application : APPLICATIONS) {
            if (!application.waitFor(10, TimeUnit.SECONDS)) {
                application.destroyForcibly();
                application.waitFor(10, TimeUnit.SECONDS);
            }
        }
        APPLICATIONS.clear();
    }
}
