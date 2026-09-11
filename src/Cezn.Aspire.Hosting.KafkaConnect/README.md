# Cezn.Aspire.Hosting.KafkaConnect

Kafka Connect hosting support for Aspire, including Debezium PostgreSQL connector.

## Usage

### Basic Kafka Connect

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka");

var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka)
    .WithDockerfile("../my-connect-image");

builder.Build().Run();
```

### With Schema Registry

```csharp
var schemaRegistry = builder.AddSchemaRegistry("schema-registry")
    .WithKafka(kafka);

var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka)
    .WithSchemaRegistry(schemaRegistry)
    .WithDockerfile("../my-connect-image");
```

### With OpenTelemetry

```csharp
var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka)
    .WithOtel()
    .WithDockerfile("../my-connect-image");
```

### Debezium PostgreSQL Connector

```csharp
var postgres = builder.AddPostgres("postgres")
    .WithImage("postgres", "16-alpine")
    .WithArgs("-c", "wal_level=logical");

var db = postgres.AddDatabase("mydb");

var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka)
    .WithDockerfile("../my-connect-image");

kafkaConnect.AddPostgresDebeziumConnector("cdc", db, options =>
{
    options.TableIncludeList = "public.Bookmarks";
    options.SlotName = "bookmarks_cdc";
    options.PublicationName = "bookmarks_publication";
});
```

## Dockerfile

A typical Dockerfile for Kafka Connect with Debezium and OTel:

```dockerfile
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:copy-dependencies -DoutputDirectory=otel-lib

FROM confluentinc/cp-kafka-connect:7.9.2

ADD --chown=appuser:appuser \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.33.0/opentelemetry-javaagent.jar \
    /otel/opentelemetry-javaagent.jar

COPY --from=builder /build/otel-lib /usr/share/java/otel-lib

RUN curl -LO \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.1.2.Final/debezium-connector-postgres-3.1.2.Final-plugin.tar.gz && \
    tar -xzf debezium-connector-postgres-3.1.2.Final-plugin.tar.gz -C /usr/share/confluent-hub-components && \
    rm -f debezium-connector-postgres-3.1.2.Final-plugin.tar.gz
```

## Container image

Defaults to `confluentinc/cp-kafka-connect:7.9.2`. Override with `.WithImage()` or `.WithDockerfile()`.
