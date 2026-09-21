package com.example.inventoryservice.observability;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.availability.AvailabilityHealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.availability.AvailabilityProbesAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.availability.ApplicationAvailabilityAutoConfiguration;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class HealthEndpointConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(context -> loadApplicationYaml(context))
            .withConfiguration(AutoConfigurations.of(
                    ApplicationAvailabilityAutoConfiguration.class,
                    AvailabilityProbesAutoConfiguration.class,
                    AvailabilityHealthContributorAutoConfiguration.class,
                    HealthContributorAutoConfiguration.class,
                    HealthEndpointAutoConfiguration.class))
            .withUserConfiguration(TestHealthIndicators.class);

    @Test
    void livenessIgnoresDatabaseAndKafkaFailures() {
        contextRunner.run(context -> {
            acceptTraffic(context);
            context.getBean("dbHealthIndicator", TestHealthIndicator.class).down();
            context.getBean("kafkaHealthIndicator", TestHealthIndicator.class).down();

            assertThat(status(context.getBean(HealthEndpoint.class), "liveness")).isEqualTo(Status.UP);
        });
    }

    @Test
    void readinessIncludesDatabaseAndKafka() {
        contextRunner.run(context -> {
            acceptTraffic(context);
            HealthEndpoint endpoint = context.getBean(HealthEndpoint.class);
            TestHealthIndicator db = context.getBean("dbHealthIndicator", TestHealthIndicator.class);
            TestHealthIndicator kafka = context.getBean("kafkaHealthIndicator", TestHealthIndicator.class);

            assertThat(status(endpoint, "readiness")).isEqualTo(Status.UP);

            kafka.down();
            assertThat(status(endpoint, "readiness")).isEqualTo(Status.DOWN);

            kafka.up();
            db.down();
            assertThat(status(endpoint, "readiness")).isEqualTo(Status.DOWN);
        });
    }

    private static void acceptTraffic(org.springframework.context.ApplicationContext context) {
        AvailabilityChangeEvent.publish(context, LivenessState.CORRECT);
        AvailabilityChangeEvent.publish(context, ReadinessState.ACCEPTING_TRAFFIC);
    }

    private static void loadApplicationYaml(org.springframework.context.ConfigurableApplicationContext context) {
        try {
            new YamlPropertySourceLoader()
                    .load("application", new ClassPathResource("application.yaml"))
                    .forEach(source -> context.getEnvironment().getPropertySources().addLast(source));
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot load application.yaml", ex);
        }
    }

    private static Status status(HealthEndpoint endpoint, String group) {
        return endpoint.healthForPath(group).getStatus();
    }

    @Configuration(proxyBeanMethods = false)
    static class TestHealthIndicators {
        @Bean
        TestHealthIndicator dbHealthIndicator() {
            return new TestHealthIndicator();
        }

        @Bean
        TestHealthIndicator kafkaHealthIndicator() {
            return new TestHealthIndicator();
        }
    }

    static class TestHealthIndicator implements HealthIndicator {
        private final AtomicReference<Health> health = new AtomicReference<>(Health.up().build());

        void up() {
            health.set(Health.up().build());
        }

        void down() {
            health.set(Health.down().build());
        }

        @Override
        public Health health() {
            return health.get();
        }
    }
}
