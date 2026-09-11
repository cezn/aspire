namespace Cezn.Aspire.Hosting;

/// <summary>
/// Contains the Dockerfile content for the Kafka Connect container image.
/// Generated programmatically to avoid filesystem dependencies in the NuGet package.
/// </summary>
internal static class KafkaConnectDockerfile
{
    internal const string Content = """
        # Stage 1: Use Maven to download OpenTelemetry JARs with transitive dependencies
        FROM maven:3.9.6-eclipse-temurin-17 AS builder

        WORKDIR /build

        # Create POM with dependencies - Maven resolves all transitive deps automatically
        RUN cat > pom.xml << 'POM'
        <project xmlns="http://maven.apache.org/POM/4.0.0"
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
            <modelVersion>4.0.0</modelVersion>
            <groupId>aspire</groupId>
            <artifactId>kafka-connect-otel</artifactId>
            <version>1.0</version>
            <properties>
                <version.opentelemetry.kafka.clients.2.6>1.23.0-alpha</version.opentelemetry.kafka.clients.2.6>
            </properties>
            <dependencies>
                <dependency>
                    <groupId>io.debezium</groupId>
                    <artifactId>debezium-interceptor</artifactId>
                    <version>3.1.2.Final</version>
                </dependency>
                <dependency>
                    <groupId>io.opentelemetry.instrumentation</groupId>
                    <artifactId>opentelemetry-kafka-clients-2.6</artifactId>
                    <version>${version.opentelemetry.kafka.clients.2.6}</version>
                </dependency>
            </dependencies>
        </project>
        POM

        RUN mvn dependency:copy-dependencies -DoutputDirectory=otel-lib

        # Stage 2: Build final Kafka Connect image
        FROM confluentinc/cp-kafka-connect:7.9.2

        # Download OpenTelemetry Java agent
        ADD --chown=appuser:appuser https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.33.0/opentelemetry-javaagent.jar /otel/opentelemetry-javaagent.jar

        # Copy OpenTelemetry libs from builder
        COPY --from=builder /build/otel-lib /usr/share/java/otel-lib

        # Download Debezium Postgres connector plugin if not present
        RUN if [ ! -f /usr/share/confluent-hub-components/debezium-connector-postgres-3.1.2.Final.jar ]; then \
          curl -LO https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.1.2.Final/debezium-connector-postgres-3.1.2.Final-plugin.tar.gz && \
          tar -xzf debezium-connector-postgres-3.1.2.Final-plugin.tar.gz -C /usr/share/confluent-hub-components && \
          rm -f debezium-connector-postgres-3.1.2.Final-plugin.tar.gz; \
          fi
        """;
}
