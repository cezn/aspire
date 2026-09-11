using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource;

/// <summary>
/// Options that control how a Debezium PostgreSQL source connector is created.
/// </summary>
public sealed class PostgresDebeziumConnectorOptions
{
    /// <summary>
    /// Gets or sets the Kafka topic prefix for emitted CDC topics.
    /// </summary>
    public required string TopicPrefix { get; set; }

    /// <summary>
    /// Gets or sets the optional schema include list (e.g. "public").
    /// </summary>
    public string? SchemaIncludeList { get; set; }

    /// <summary>
    /// Gets or sets the optional table include list (e.g. "public.Bookmarks").
    /// </summary>
    public string? TableIncludeList { get; set; }

    /// <summary>
    /// Gets or sets the replication slot name.
    /// </summary>
    public string? SlotName { get; set; }

    /// <summary>
    /// Gets or sets the publication name.
    /// </summary>
    public string? PublicationName { get; set; }

    /// <summary>
    /// Gets or sets the snapshot mode.
    /// </summary>
    public string SnapshotMode { get; set; } = "initial";

    /// <summary>
    /// Gets or sets the Debezium heartbeat interval in milliseconds.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Gets or sets whether transaction metadata should be emitted.
    /// </summary>
    public bool ProvideTransactionMetadata { get; set; } = true;

    /// <summary>
    /// Gets or sets the SMT class for the route transform
    /// (e.g. "org.apache.kafka.connect.transforms.RegexRouter").
    /// </summary>
    public string? TransformsRouteType { get; set; }

    /// <summary>
    /// Gets or sets the regex pattern for the route transform
    /// (e.g. "([^.]+)\\.([^.]+)\\.([^.]+)").
    /// </summary>
    public string? TransformsRouteRegex { get; set; }

    /// <summary>
    /// Gets or sets the replacement string for the route transform
    /// (e.g. "$3").
    /// </summary>
    public string? TransformsRouteReplacement { get; set; }

    /// <summary>
    /// Gets or sets the key converter class (e.g. "io.confluent.connect.protobuf.ProtobufConverter").
    /// </summary>
    public string? KeyConverter { get; set; }

    /// <summary>
    /// Gets or sets the value converter class (e.g. "io.debezium.converters.BinaryDataConverter").
    /// </summary>
    public string? ValueConverter { get; set; }

    /// <summary>
    /// Gets or sets the SMT (Single Message Transform) pipeline name (e.g. "outbox").
    /// </summary>
    public string? Transforms { get; set; }

    /// <summary>
    /// Gets or sets the Schema Registry URL for the key converter (e.g. for ProtobufConverter).
    /// </summary>
    public EndpointReference? KeyConverterSchemaRegistryUrl { get; set; }

    /// <summary>
    /// Gets or sets the Schema Registry URL for the value converter.
    /// </summary>
    public string? ValueConverterSchemaRegistryUrl { get; set; }

    /// <summary>
    /// Gets or sets the delegate converter type for the value converter
    /// (e.g. "org.apache.kafka.connect.json.JsonConverter" when using BinaryDataConverter).
    /// </summary>
    public string? ValueConverterDelegateConverterType { get; set; }

    /// <summary>
    /// Gets or sets whether schemas are enabled for the value converter delegate.
    /// </summary>
    public bool? ValueConverterDelegateConverterSchemasEnable { get; set; }

    /// <summary>
    /// Gets or sets the SMT class type for the outbox EventRouter transform
    /// (e.g. "io.debezium.transforms.outbox.EventRouter").
    /// </summary>
    public string? TransformsOutboxType { get; set; }

    /// <summary>
    /// Gets or sets the SMT event key field for the EventRouter transform (e.g. "user_id").
    /// </summary>
    public string? TransformsOutboxTableFieldEventKey { get; set; }

    /// <summary>
    /// Gets or sets the additional field placement configuration for the EventRouter transform
    /// (e.g. "type:header:type").
    /// </summary>
    public string? TransformsOutboxTableFieldsAdditionalPlacement { get; set; }

    /// <summary>
    /// Gets or sets the tracing span context field name for distributed tracing propagation.
    /// </summary>
    public string? TracingSpanContextField { get; set; }

    /// <summary>
    /// Gets or sets whether tracing should use the context field only.
    /// </summary>
    public bool? TracingWithContextFieldOnly { get; set; }

    /// <summary>
    /// Gets additional arbitrary connector configuration key-value pairs.
    /// These are merged into the final connector config, allowing any Debezium option to be set.
    /// </summary>
    public IDictionary<string, string> ConnectorConfig { get; } = new Dictionary<string, string>();
}
