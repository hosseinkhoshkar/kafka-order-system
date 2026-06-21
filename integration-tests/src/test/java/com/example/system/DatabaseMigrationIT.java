package com.example.system;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class DatabaseMigrationIT {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15.6")
            .withDatabaseName("migrationdb");

    private static final Path ROOT = Path.of(System.getProperty("repositoryRoot")).toAbsolutePath().normalize();
    private static final String ORDER_MIGRATIONS = "filesystem:" +
            ROOT.resolve("order-service/src/main/resources/db/migration");
    private static final String INVENTORY_MIGRATIONS = "filesystem:" +
            ROOT.resolve("inventory-service/src/main/resources/db/migration");

    @BeforeEach
    void resetDatabase() throws Exception {
        try (Connection connection = connection()) {
            execute(connection, "DROP SCHEMA public CASCADE");
            execute(connection, "CREATE SCHEMA public");
        }
    }

    @Test
    void orderMigrationsCreateEmptyDatabaseAndAreRepeatable() throws Exception {
        Flyway flyway = flyway(ORDER_MIGRATIONS);

        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(3);
        assertThat(flyway.migrate().migrationsExecuted).isZero();

        try (Connection connection = connection()) {
            assertThat(columnType(connection, "orders", "price")).isEqualTo("numeric");
            assertThat(indexExists(connection, "idx_outbox_events_status")).isTrue();
            assertThat(indexExists(connection, "idx_event_store_aggregate_version")).isTrue();

            execute(connection, """
                    INSERT INTO orders(order_id, product_id, customer_id, quantity, price, status, created_at, updated_at)
                    VALUES ('order-ok', 'product-ok', 'customer-ok', 1, 10.25, 'PENDING', now(), now())
                    """);
            assertThatThrownBy(() -> execute(connection, """
                    INSERT INTO orders(order_id, product_id, customer_id, quantity, price, status, created_at, updated_at)
                    VALUES ('order-bad-qty', 'product-ok', 'customer-ok', 0, 10.25, 'PENDING', now(), now())
                    """)).isInstanceOf(SQLException.class);
            assertThatThrownBy(() -> execute(connection, """
                    INSERT INTO orders(order_id, product_id, customer_id, quantity, price, status, created_at, updated_at)
                    VALUES ('order-bad-price', 'product-ok', 'customer-ok', 1, 0.00, 'PENDING', now(), now())
                    """)).isInstanceOf(SQLException.class);
        }
    }

    @Test
    void inventoryMigrationsCreateEmptyDatabaseAndAreRepeatable() throws Exception {
        Flyway flyway = flyway(INVENTORY_MIGRATIONS);

        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(3);
        assertThat(flyway.migrate().migrationsExecuted).isZero();

        try (Connection connection = connection()) {
            assertThat(indexExists(connection, "idx_event_store_aggregate_version")).isTrue();
            assertThat(indexExists(connection, "idx_inventory_outbox_claim_available")).isTrue();
            assertThat(indexExists(connection, "idx_inventory_outbox_claim_expired")).isTrue();
            execute(connection, """
                    INSERT INTO inventory(product_id, available_quantity, reserved_quantity)
                    VALUES ('product-ok', 10, 0)
                    """);
            assertThatThrownBy(() -> execute(connection, """
                    INSERT INTO inventory(product_id, available_quantity, reserved_quantity)
                    VALUES ('product-bad', -1, 0)
                    """)).isInstanceOf(SQLException.class);
        }
    }

    @Test
    void legacyOrderSchemaUpgradePreservesCompatibleData() throws Exception {
        try (Connection connection = connection()) {
            createLegacyOrderSchema(connection);
            execute(connection, """
                    INSERT INTO orders(order_id, product_id, customer_id, quantity, price, status, created_at, updated_at)
                    VALUES ('legacy-order', 'product-1', 'customer-1', 2, 12.34, 'PENDING', now(), now())
                    """);
            execute(connection, """
                    INSERT INTO outbox_events(id, aggregate_id, aggregate_type, event_type, payload, status, created_at, sent_at)
                    VALUES ('outbox-1', 'legacy-order', 'ORDER', 'ORDER_CREATED', '{}', 'PENDING', now(), NULL)
                    """);
            execute(connection, """
                    INSERT INTO event_store(id, aggregate_id, aggregate_type, event_type, payload, version, occurred_at)
                    VALUES ('event-1', 'legacy-order', 'ORDER', 'ORDER_CREATED', '{}', 1, now())
                    """);
        }

        baselineThenMigrate(ORDER_MIGRATIONS);

        try (Connection connection = connection()) {
            assertThat(columnType(connection, "orders", "price")).isEqualTo("numeric");
            assertThat(new BigDecimal(scalar(connection,
                    "SELECT price FROM orders WHERE order_id = 'legacy-order'")))
                    .isEqualByComparingTo("12.34");
            assertThat(indexExists(connection, "idx_outbox_events_status")).isTrue();
            assertThat(indexExists(connection, "idx_event_store_aggregate_version")).isTrue();
        }
    }

    @Test
    void legacyOrderSchemaUpgradeRejectsIncompatiblePrices() throws Exception {
        try (Connection connection = connection()) {
            createLegacyOrderSchema(connection);
            execute(connection, """
                    INSERT INTO orders(order_id, product_id, customer_id, quantity, price, status, created_at, updated_at)
                    VALUES ('legacy-order', 'product-1', 'customer-1', 2, 12.345, 'PENDING', now(), now())
                    """);
        }

        Flyway flyway = flyway(ORDER_MIGRATIONS);
        flyway.baseline();
        assertThatThrownBy(flyway::migrate)
                .hasMessageContaining("Cannot migrate orders.price to numeric(19,2)");
    }

    @Test
    void legacyInventorySchemaUpgradePreservesCompatibleData() throws Exception {
        try (Connection connection = connection()) {
            createLegacyInventorySchema(connection);
            execute(connection, """
                    INSERT INTO inventory(product_id, available_quantity, reserved_quantity)
                    VALUES ('product-1', 10, 2)
                    """);
            execute(connection, """
                    INSERT INTO event_store(id, aggregate_id, aggregate_type, event_type, payload, version, occurred_at)
                    VALUES ('event-1', 'order-1', 'INVENTORY', 'INVENTORY_RESERVED', '{}', 1, now())
                    """);
        }

        baselineThenMigrate(INVENTORY_MIGRATIONS);

        try (Connection connection = connection()) {
            assertThat(scalar(connection, "SELECT available_quantity FROM inventory WHERE product_id = 'product-1'"))
                    .isEqualTo("10");
            assertThat(indexExists(connection, "idx_event_store_aggregate_version")).isTrue();
            execute(connection, """
                    INSERT INTO inventory_inbox_events(event_id, event_type, aggregate_id, payload_hash, received_at)
                    VALUES ('event-1', 'ORDER_CREATED', 'order-1', repeat('a', 64), now())
                    """);
            execute(connection, """
                    INSERT INTO inventory_reservation_decisions(
                        order_id, source_event_id, product_id, quantity, status,
                        failure_reason, response_event_id, decided_at)
                    VALUES ('order-1', 'event-1', 'product-1', 2, 'RESERVED',
                        NULL, 'reply-1', now())
                    """);
            execute(connection, """
                    INSERT INTO inventory_outbox_events(
                        id, aggregate_id, aggregate_type, event_type, payload, status,
                        created_at, attempt_count, next_attempt_at)
                    VALUES ('reply-1', 'order-1', 'INVENTORY', 'INVENTORY_RESERVED',
                        '{}', 'PENDING', now(), 0, now())
                    """);
        }
    }

    private static Flyway flyway(String location) {
        return Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations(location)
                .baselineVersion("1")
                .load();
    }

    private static void baselineThenMigrate(String location) {
        Flyway flyway = flyway(location);
        flyway.baseline();
        flyway.migrate();
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void createLegacyOrderSchema(Connection connection) throws SQLException {
        execute(connection, """
                CREATE TABLE orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    product_id VARCHAR(255),
                    customer_id VARCHAR(255),
                    quantity INTEGER,
                    price DOUBLE PRECISION,
                    status VARCHAR(255),
                    created_at TIMESTAMP(6),
                    updated_at TIMESTAMP(6)
                )
                """);
        execute(connection, """
                CREATE TABLE outbox_events (
                    id VARCHAR(255) PRIMARY KEY,
                    aggregate_id VARCHAR(255),
                    aggregate_type VARCHAR(255),
                    event_type VARCHAR(255),
                    payload TEXT,
                    status VARCHAR(255),
                    created_at TIMESTAMP(6),
                    sent_at TIMESTAMP(6)
                )
                """);
        execute(connection, """
                CREATE TABLE event_store (
                    id VARCHAR(255) PRIMARY KEY,
                    aggregate_id VARCHAR(255),
                    aggregate_type VARCHAR(255),
                    event_type VARCHAR(255),
                    payload TEXT,
                    version INTEGER,
                    occurred_at TIMESTAMP(6)
                )
                """);
    }

    private static void createLegacyInventorySchema(Connection connection) throws SQLException {
        execute(connection, """
                CREATE TABLE inventory (
                    product_id VARCHAR(255) PRIMARY KEY,
                    available_quantity INTEGER,
                    reserved_quantity INTEGER
                )
                """);
        execute(connection, """
                CREATE TABLE event_store (
                    id VARCHAR(255) PRIMARY KEY,
                    aggregate_id VARCHAR(255),
                    aggregate_type VARCHAR(255),
                    event_type VARCHAR(255),
                    payload TEXT,
                    version INTEGER,
                    occurred_at TIMESTAMP(6)
                )
                """);
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String scalar(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement();
             var rows = statement.executeQuery(sql)) {
            return rows.next() ? rows.getString(1) : null;
        }
    }

    private static String columnType(Connection connection, String table, String column) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = ?
                  AND column_name = ?
                """)) {
            statement.setString(1, table);
            statement.setString(2, column);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    private static boolean indexExists(Connection connection, String indexName) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = ?
                """)) {
            statement.setString(1, indexName);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }
}
