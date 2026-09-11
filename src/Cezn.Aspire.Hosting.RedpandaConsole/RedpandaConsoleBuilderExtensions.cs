using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for adding a Redpanda Console container resource to an Aspire application.
/// </summary>
public static class RedpandaConsoleBuilderExtensions
{
    /// <summary>
    /// Adds a Redpanda Console container resource to the application.
    /// A <c>KafkaServerResource</c> should be referenced via <see cref="WithKafka"/>.
    /// Optionally connect to a <c>SchemaRegistryResource</c> via <see cref="WithSchemaRegistry"/>
    /// and/or a <c>KafkaConnectResource</c> via <see cref="WithKafkaConnect"/>.
    /// </summary>
    /// <param name="builder">The Aspire application builder.</param>
    /// <param name="name">The logical resource name.</param>
    /// <returns>A resource builder for the Redpanda Console.</returns>
    public static IResourceBuilder<RedpandaConsoleResource> AddRedpandaConsole(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var resource = new RedpandaConsoleResource(name);

        return builder
            .AddResource(resource)
            .WithImage(RedpandaConsoleContainerImageTags.Image, RedpandaConsoleContainerImageTags.Tag)
            .WithHttpEndpoint(targetPort: 8080)
            .WithHttpHealthCheck()
            .WithEntrypoint("/bin/sh")
            .WithArgs("-c", "echo \"$CONSOLE_CONFIG_FILE\" > /tmp/config.yml; /app/console");
    }

    /// <summary>
    /// Configures the Redpanda Console to connect to a Kafka broker.
    /// </summary>
    /// <param name="builder">The Redpanda Console resource builder.</param>
    /// <param name="kafka">The Kafka broker resource to connect to.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<RedpandaConsoleResource> WithKafka(
        this IResourceBuilder<RedpandaConsoleResource> builder,
        IResourceBuilder<KafkaServerResource> kafka
    )
    {
        builder.Resource.Kafka = kafka.Resource;
        builder.ApplyConfigEnvironment();

        return builder;
    }

    /// <summary>
    /// Configures the Redpanda Console to connect to a Schema Registry.
    /// </summary>
    /// <param name="builder">The Redpanda Console resource builder.</param>
    /// <param name="schemaRegistry">The Schema Registry resource.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<RedpandaConsoleResource> WithSchemaRegistry(
        this IResourceBuilder<RedpandaConsoleResource> builder,
        IResourceBuilder<SchemaRegistryResource> schemaRegistry
    )
    {
        builder.Resource.SchemaRegistry = schemaRegistry.Resource;
        builder.ApplyConfigEnvironment();

        return builder;
    }

    /// <summary>
    /// Configures the Redpanda Console to connect to a Kafka Connect instance.
    /// </summary>
    /// <param name="builder">The Redpanda Console resource builder.</param>
    /// <param name="kafkaConnect">The Kafka Connect resource.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<RedpandaConsoleResource> WithKafkaConnect(
        this IResourceBuilder<RedpandaConsoleResource> builder,
        IResourceBuilder<KafkaConnectResource> kafkaConnect
    )
    {
        builder.Resource.KafkaConnect = kafkaConnect.Resource;
        builder.ApplyConfigEnvironment();

        return builder;
    }

    /// <summary>
    /// Builds the Redpanda Console YAML config from all currently-set dependencies
    /// and applies it as environment variables.
    /// </summary>
    static IResourceBuilder<RedpandaConsoleResource> ApplyConfigEnvironment(
        this IResourceBuilder<RedpandaConsoleResource> builder
    )
    {
        var resource = builder.Resource;

        return builder.WithEnvironment(ctx =>
        {
            var env = ctx.EnvironmentVariables;

            var configBuilder = new ReferenceExpressionBuilder();

            if (resource.Kafka is { } kafka)
            {
                var kafkaEndpoint = kafka.InternalEndpoint;
                configBuilder.Append(
                    $"""
                    kafka:
                      brokers: [{kafkaEndpoint.Property(EndpointProperty.HostAndPort)}]

                    """
                );
            }

            if (resource.SchemaRegistry is { } schemaRegistry)
            {
                configBuilder.Append(
                    $"""
                    schemaRegistry:
                      enabled: true
                      urls: [{schemaRegistry.HttpEndpoint}]

                    """
                );
            }

            if (resource.KafkaConnect is { } kafkaConnect)
            {
                configBuilder.Append(
                    $"""
                    kafkaConnect:
                      enabled: true
                      clusters:
                        - name: kafka-connect
                          url: {kafkaConnect.HttpEndpoint}

                    """
                );
            }

            env["CONFIG_FILEPATH"] = "/tmp/config.yml";
            env["CONSOLE_CONFIG_FILE"] = configBuilder.Build();
        });
    }
}
