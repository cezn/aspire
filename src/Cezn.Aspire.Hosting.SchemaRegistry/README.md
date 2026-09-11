# Cezn.Aspire.Hosting.SchemaRegistry

Confluent Schema Registry hosting support for Aspire.

## Usage

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka");

var schemaRegistry = builder.AddSchemaRegistry("schema-registry")
    .WithKafka(kafka);

builder.Build().Run();
```

## Configuration

| Environment variable | Description |
|---|---|
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | Kafka bootstrap servers (set automatically by `WithKafka`) |
| `SCHEMA_REGISTRY_HOST_NAME` | Host name (set automatically) |
| `SCHEMA_REGISTRY_LISTENERS` | Listener URL (set automatically) |

## Container image

Defaults to `confluentinc/cp-schema-registry:7.6.1`. Override with `.WithImage()`.
