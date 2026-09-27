# Cezn.Aspire.Hosting.KafkaTopic

Kafka topic creation hosting support for Aspire. Adds a topic resource to a Kafka
broker and creates the topic on the broker during AppHost startup (or on demand via
the dashboard command).

## Usage

```csharp
var kafka = builder.AddKafka("kafka");

var outboxTopic = kafka
    .AddKafkaTopic("outbox-topic", topicName: "outbox.event.Bookmark")
    .WithPartitions(3)
    .WithReplicationFactor(1)
    .CreateTopicOnStart();
```

- `AddKafkaTopic(name, topicName)` — declares the topic resource. The topic name
  defaults to the resource name.
- `WithPartitions(n)` / `WithReplicationFactor(n)` — topic configuration.
- `CreateTopicOnStart()` — creates the topic automatically once the Kafka broker is
  ready. Uses `kafka-topics --create --if-not-exists` inside the Kafka container, so
  it is idempotent across restarts.

The resource also exposes a **Create Topic** command in the Aspire dashboard for
manual re-creation.
