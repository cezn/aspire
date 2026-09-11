using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource;

/// <summary>
/// Extension methods for <see cref="PostgresDebeziumConnectorOptions"/>.
/// </summary>
public static class PostgresDebeziumConnectorOptionsExtensions
{
    /// <summary>
    /// Configures the connector for the Debezium outbox CDC pattern.
    /// </summary>
    /// <remarks>
    /// The Debezium Postgres Source Connector uses PostgreSQL logical replication (CDC) to capture
    /// row-level changes and publish them to Kafka topics. For the outbox pattern, the source database
    /// must contain an <c>outbox</c> table with the following schema:
    /// <code>
    /// create table outbox
    /// (
    ///   id uuid not null primary key,
    ///   aggregatetype varchar(255) not null,
    ///   aggregateid varchar(255) not null,
    ///   user_id varchar(255) not null,
    ///   type varchar(255) not null,
    ///   payload bytea,
    ///   tracingspancontext text
    /// );
    /// </code>
    /// This method configures the connector with the EventRouter SMT, Protobuf key converter,
    /// and BinaryData value converter with a JSON delegate, suitable for emitting outbox events
    /// to Kafka topics routed by event type.
    /// </remarks>
    /// <param name="options">The connector options to configure.</param>
    /// <param name="schemaRegistryUrl">
    /// The Schema Registry endpoint reference for Protobuf key converter schema registration.
    /// </param>
    /// <param name="topicPrefix">
    /// The Kafka topic prefix. Defaults to <c>"outbox"</c>.
    /// </param>
    /// <param name="tableIncludeList">
    /// The table include list filter. Defaults to <c>"public.outbox"</c>.
    /// </param>
    /// <param name="eventKeyField">
    /// The outbox table field to use as the event key for the EventRouter transform.
    /// Defaults to <c>"user_id"</c>.
    /// </param>
    /// <param name="tracingSpanContextField">
    /// The outbox table field name that carries tracing span context for distributed tracing propagation.
    /// Defaults to <c>"tracingspancontext"</c>.
    /// </param>
    /// <returns>The configured options for chaining.</returns>
    public static PostgresDebeziumConnectorOptions WithOutboxCdc(
        this PostgresDebeziumConnectorOptions options,
        EndpointReference schemaRegistryUrl,
        string topicPrefix = "outbox",
        string tableIncludeList = "public.outbox",
        string eventKeyField = "user_id",
        string tracingSpanContextField = "tracingspancontext"
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schemaRegistryUrl);

        options.TopicPrefix = topicPrefix;
        options.TableIncludeList = tableIncludeList;
        options.SlotName = $"{topicPrefix}_slot";
        options.PublicationName = $"{topicPrefix}_publication";
        options.KeyConverter = "io.confluent.connect.protobuf.ProtobufConverter";
        options.KeyConverterSchemaRegistryUrl = schemaRegistryUrl;
        options.ValueConverter = "io.debezium.converters.BinaryDataConverter";
        options.ValueConverterDelegateConverterType = "org.apache.kafka.connect.json.JsonConverter";
        options.ValueConverterDelegateConverterSchemasEnable = false;
        options.Transforms = "outbox";
        options.TransformsOutboxType = "io.debezium.transforms.outbox.EventRouter";
        options.TransformsOutboxTableFieldEventKey = eventKeyField;
        options.TransformsOutboxTableFieldsAdditionalPlacement = "type:header:type";
        options.TracingSpanContextField = tracingSpanContextField;
        options.TracingWithContextFieldOnly = false;

        return options;
    }
}
