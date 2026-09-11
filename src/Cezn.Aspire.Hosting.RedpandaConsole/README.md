# Cezn.Aspire.Hosting.RedpandaConsole

Redpanda Console hosting support for Aspire.

## Usage

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka");
var schemaRegistry = builder.AddSchemaRegistry("schema-registry")
    .WithKafka(kafka);
var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka);

var console = builder.AddRedpandaConsole("console", kafka, schemaRegistry, kafkaConnect);

builder.Build().Run();
```

## Container image

Defaults to `docker.redpanda.com/redpandadata/console:v3.0.1`. Override with `.WithImage()`.
