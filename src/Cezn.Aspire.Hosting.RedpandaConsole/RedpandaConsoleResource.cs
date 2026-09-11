using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// A container resource representing a Redpanda Console (formerly Redpanda Manager) instance.
/// </summary>
/// <param name="name">The logical resource name.</param>
public sealed class RedpandaConsoleResource([ResourceName] string name) : ContainerResource(name)
{
    /// <summary>
    /// The Kafka broker resource, set via <c>WithKafka</c>.
    /// </summary>
    internal KafkaServerResource? Kafka { get; set; }

    /// <summary>
    /// The Schema Registry resource, set via <c>WithSchemaRegistry</c>.
    /// </summary>
    internal SchemaRegistryResource? SchemaRegistry { get; set; }

    /// <summary>
    /// The Kafka Connect resource, set via <c>WithKafkaConnect</c>.
    /// </summary>
    internal KafkaConnectResource? KafkaConnect { get; set; }
}
