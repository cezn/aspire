using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for adding a Confluent Schema Registry container resource to an Aspire application.
/// </summary>
public static class SchemaRegistryBuilderExtensions
{
    /// <summary>
    /// Adds a Confluent Schema Registry container resource to the application.
    /// A <c>KafkaServerResource</c> should be referenced via <see cref="WithKafka"/> or <see cref="WithReference{T}"/>.
    /// </summary>
    /// <param name="builder">The Aspire application builder.</param>
    /// <param name="name">The logical resource name.</param>
    /// <returns>A resource builder for the Schema Registry.</returns>
    public static IResourceBuilder<SchemaRegistryResource> AddSchemaRegistry(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var resource = new SchemaRegistryResource(name);

        return builder
            .AddResource(resource)
            .WithImage(SchemaRegistryContainerImageTags.Image, SchemaRegistryContainerImageTags.Tag)
            .WithHttpEndpoint(name: SchemaRegistryResource.HttpEndpointName, targetPort: 8081)
            .WithHttpHealthCheck("/schemas");
    }

    /// <summary>
    /// Configures the Schema Registry to connect to a Kafka broker.
    /// </summary>
    /// <param name="builder">The Schema Registry resource builder.</param>
    /// <param name="kafka">The Kafka broker resource to connect to.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<SchemaRegistryResource> WithKafka(
        this IResourceBuilder<SchemaRegistryResource> builder,
        IResourceBuilder<KafkaServerResource> kafka
    )
    {
        var kafkaPlaintextEndpoint = kafka.Resource.InternalEndpoint;
        var kafkaHttpEndpoint = builder.Resource.HttpEndpoint;

        return builder
            .WithEnvironment(ctx =>
            {
                ctx.EnvironmentVariables["SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"] =
                    ReferenceExpression.Create(
                        $"PLAINTEXT://{kafkaPlaintextEndpoint.Property(EndpointProperty.HostAndPort)}"
                    );
                ctx.EnvironmentVariables["SCHEMA_REGISTRY_HOST_NAME"] = kafkaHttpEndpoint.Property(
                    EndpointProperty.Host
                );
                ctx.EnvironmentVariables["SCHEMA_REGISTRY_LISTENERS"] = kafkaHttpEndpoint;
                ctx.EnvironmentVariables["SCHEMA_REGISTRY_DEBUG"] = "false";
                // Force compact cleanup policy on the _schemas topic so schemas are never time-based deleted.
                ctx.EnvironmentVariables["SCHEMA_REGISTRY_KAFKASTORE_TOPIC_CONFIGS"] =
                    "cleanup.policy=compact";
            });
    }
}
