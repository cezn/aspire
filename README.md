## Description

This repository contains a collection of Aspire hosting packages for the Kafka/Redpanda ecosystem.
Currently, these include:
- **Schema Registry** — container-based Confluent Schema Registry resource for Kafka.
- **Redpanda Console** — Redpanda Console (formerly Redpanda Manager) UI resource.
- **Kafka Connect (Debezium Postgres)** — Kafka Connect with Debezium PostgreSQL CDC connector resource.

## Project structure

The repository follows the same layout as the [Aspire](https://github.com/aspire) source repository, where each hosting package is a self-contained project under `src/`.

```
├── src/
│   ├── Cezn.Aspire.Hosting.SchemaRegistry/
│   │   ├── Cezn.Aspire.Hosting.SchemaRegistry.csproj
│   │   ├── SchemaRegistryBuilderExtensions.cs   # AddSchemaRegistry(), WithKafka()
│   │   ├── SchemaRegistryResource.cs             # Resource model (IResourceWithConnectionString)
│   │   ├── SchemaRegistryContainerImageTags.cs   # Container image constants
│   │   └── README.md
│   │
│   ├── Cezn.Aspire.Hosting.RedpandaConsole/
│   │   ├── Cezn.Aspire.Hosting.RedpandaConsole.csproj
│   │   ├── RedpandaConsoleBuilderExtensions.cs   # AddRedpandaConsole()
│   │   ├── RedpandaConsoleResource.cs            # Resource model
│   │   ├── RedpandaConsoleContainerImageTags.cs  # Container image constants
│   │   └── README.md
│   │
│   └── Cezn.Aspire.Hosting.KafkaConnect/
│       ├── Cezn.Aspire.Hosting.KafkaConnect.csproj
│       ├── KafkaConnectBuilderExtensions.cs      # AddKafkaConnect(), WithKafka(), WithSchemaRegistry(), WithOtel()
│       ├── KafkaConnectResource.cs               # Resource model (IResourceWithConnectionString)
│       ├── KafkaConnectContainerImageTags.cs     # Container image constants
│       ├── PostgresDebeziumConnectorOptions.cs   # Connector configuration options
│       ├── PostgresDebeziumConnectorResource.cs  # Parent resource (IResourceWithParent<KafkaConnectResource>)
│       ├── PostgresDebeziumConnectorBuilderExtensions.cs  # AddPostgresDebeziumConnector()
│       ├── PostgresDebeziumConnectorEventingSubscriber.cs # REST API reconciliation + dashboard commands
│       └── README.md
│
├── Directory.Build.props                          # Shared build properties (TFM, nullable, packaging)
├── Directory.Build.targets                        # Shared build targets
├── Directory.Packages.props                       # Centralized package versions
└── Cezn.Aspire.sln                               # Solution file
```

### Dependencies

```
Cezn.Aspire.Hosting.SchemaRegistry  ──┐
                                       ├── Aspire.Hosting.Kafka (official)
Cezn.Aspire.Hosting.KafkaConnect  ────┤── Aspire.Hosting.Docker (official)
                                       │
                                       ├── Cezn.Aspire.Hosting.SchemaRegistry
Cezn.Aspire.Hosting.RedpandaConsole ──┤── Cezn.Aspire.Hosting.KafkaConnect
                                       └── Aspire.Hosting.Kafka (official)
```

### Design decisions vs. original playground

| Aspect | Original | Migrated |
|---|---|---|
| Kafka broker | Custom `KafkaResource` in `Broker/` | Official `Aspire.Hosting.Kafka` (`AddKafka()`) |
| Single project | All code in one shared project | Split into 3 independent NuGet packages |
| Cross-references | Hard namespace dependencies | Fluent `WithKafka()`, `WithSchemaRegistry()` extensions |
| Dockerfile path | Hardcoded relative path | User-applied `.WithDockerfile()` (no library coupling) |
| Namespace | `AspirePlayground.Kafka.*` | Unified `Cezn.Aspire.Hosting` |

### Package pattern

Each hosting package follows the standard Aspire integration pattern (mirroring e.g. `Aspire.Hosting.Kafka`, `Aspire.Hosting.Redis`):

| File | Purpose |
|---|---|
| `*BuilderExtensions.cs` | `Add<...>()` and `With<...>()` extension methods; wire up containers, endpoints, health checks, and connection strings |
| `*Resource.cs` | Strongly-typed resource class deriving from `ContainerResource`, implementing `IResourceWithConnectionString` |
| `*ContainerImageTags.cs` | `const` fields for container image name, tag, and registry |
| `README.md` | Package-specific usage documentation |

### Usage example

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka");

var schemaRegistry = builder.AddSchemaRegistry("schema-registry")
    .WithKafka(kafka);

var kafkaConnect = builder.AddKafkaConnect("kafka-connect")
    .WithKafka(kafka)
    .WithSchemaRegistry(schemaRegistry);
// .WithDockerfile("../kafka-connect-image"); // optional: build from Dockerfile

var console = builder.AddRedpandaConsole("console", kafka, schemaRegistry, kafkaConnect);

builder.Build().Run();
```

### Debezium CDC example

```csharp
var postgres = builder.AddPostgres("postgres")
    .WithImage("postgres", "16-alpine")
    .WithArgs("-c", "wal_level=logical");

var db = postgres.AddDatabase("mydb");

kafkaConnect.AddPostgresDebeziumConnector("cdc", db, options =>
{
    options.TableIncludeList = "public.Bookmarks";
    options.SlotName = "bookmarks_cdc";
});
```