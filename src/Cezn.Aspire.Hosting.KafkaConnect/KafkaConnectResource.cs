using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// A container resource representing a Kafka Connect instance.
/// </summary>
/// <param name="name">The logical resource name.</param>
public sealed class KafkaConnectResource([ResourceName] string name)
    : ContainerResource(name),
        IResourceWithConnectionString
{
    internal const string HttpEndpointName = "http";

    /// <summary>
    /// Gets the HTTP endpoint for the Kafka Connect REST API.
    /// </summary>
    public EndpointReference HttpEndpoint => field ??= new(this, HttpEndpointName);

    /// <summary>
    /// Gets the connection string expression for the Kafka Connect REST API.
    /// </summary>
    public ReferenceExpression ConnectionStringExpression =>
        ReferenceExpression.Create($"{HttpEndpoint}");
}
