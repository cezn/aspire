# Cezn.Aspire.Hosting.Monitoring

Monitoring stack (Prometheus, Grafana and a Kafka JMX exporter) hosting support for Aspire.

Mirrors the plain Docker Compose setup in `containers/prometheus`, `containers/grafana`
and `containers/kafka/jmx-exporter`:

- **Kafka JMX exporter** (`bitnami/jmx-exporter`) scrapes the broker's JMX port (5555) and
  exposes Prometheus metrics on port 8080.
- **Prometheus** (`prom/prometheus`) scrapes the JMX exporter.
- **Grafana** (`grafana/grafana`) uses Prometheus as its default datasource and ships
  Kafka dashboards (KRaft, Kafka cluster) plus a .NET service dashboard.

## Usage

```csharp
var kafka = builder.AddKafka("kafka");

var jmxExporter = builder.AddKafkaJmxExporter("kafka-metrics").WithKafka(kafka);
var prometheus = builder.AddPrometheus("prometheus").WithScrapeTarget(jmxExporter);
var grafana = builder.AddGrafana("grafana").WithPrometheus(prometheus);
```

`WithKafka` enables JMX on the broker (`KAFKA_JMX_OPTS`) and rewrites the exporter's
`hostPort` to the broker's internal host and JMX port.
