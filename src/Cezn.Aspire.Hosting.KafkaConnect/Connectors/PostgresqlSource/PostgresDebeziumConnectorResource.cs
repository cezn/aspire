using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource;

/// <summary>
/// A custom resource that models a Debezium PostgreSQL source connector managed by Kafka Connect.
/// </summary>
public sealed class PostgresDebeziumConnectorResource(
    [ResourceName] string name,
    KafkaConnectResource parentResource,
    PostgresDatabaseResource sourceDatabase,
    PostgresDebeziumConnectorOptions options
) : Resource(name), IResourceWithParent<KafkaConnectResource>, IResourceWithWaitSupport
{
    /// <summary>
    /// Gets the parent Kafka Connect resource.
    /// </summary>
    public KafkaConnectResource Parent { get; } = parentResource;

    /// <summary>
    /// Gets the source database resource.
    /// </summary>
    public PostgresDatabaseResource SourceDatabase { get; } = sourceDatabase;

    /// <summary>
    /// Gets the connector options.
    /// </summary>
    public PostgresDebeziumConnectorOptions Options { get; } = options;
}
