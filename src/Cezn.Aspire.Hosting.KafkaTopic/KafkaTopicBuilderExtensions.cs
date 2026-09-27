using Aspire.Hosting.ApplicationModel;
using Cezn.Aspire.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aspire.Hosting;

/// <summary>
/// Extension methods for adding Kafka topic resources to Aspire applications.
/// </summary>
public static class KafkaTopicBuilderExtensions
{
    /// <summary>
    /// Adds a Kafka topic resource to a Kafka broker. The topic is created on the broker
    /// when the AppHost starts (see <see cref="CreateTopicOnStart"/>).
    /// </summary>
    /// <param name="builder">The Kafka broker resource builder.</param>
    /// <param name="name">The logical resource name. Also used as the topic name unless overridden.</param>
    /// <param name="topicName">The Kafka topic name. Defaults to <paramref name="name"/>.</param>
    public static IResourceBuilder<KafkaTopicResource> AddKafkaTopic(
        this IResourceBuilder<KafkaServerResource> builder,
        [ResourceName] string name,
        string? topicName = null
    )
    {
        var resource = new KafkaTopicResource(name) { TopicName = topicName ?? name, KafkaResource = builder.Resource };

        var resourceBuilder = builder
            .ApplicationBuilder.AddResource(resource)
            .WithParentRelationship(builder)
            .WithInitialState(new CustomResourceSnapshot { ResourceType = "KafkaTopic", Properties = [] })
            .WithIconName("Database")
            .WaitFor(builder);

        resourceBuilder.WithCommand(
            name: "kafka-create-topic",
            displayName: "Create Topic",
            executeCommand: context => HandleCommandAsync(context, resource),
            commandOptions: new CommandOptions
            {
                Description = "Create the Kafka topic on the broker",
                IconName = "Plus",
                IconVariant = IconVariant.Regular,
                UpdateState = context => GetCommandState(resource),
            }
        );

        return resourceBuilder;
    }

    /// <summary>
    /// Sets the number of partitions for the Kafka topic.
    /// </summary>
    public static IResourceBuilder<KafkaTopicResource> WithPartitions(
        this IResourceBuilder<KafkaTopicResource> builder,
        int partitions
    )
    {
        builder.Resource.Partitions = partitions;
        return builder;
    }

    /// <summary>
    /// Sets the replication factor for the Kafka topic.
    /// </summary>
    public static IResourceBuilder<KafkaTopicResource> WithReplicationFactor(
        this IResourceBuilder<KafkaTopicResource> builder,
        int replicationFactor
    )
    {
        builder.Resource.ReplicationFactor = replicationFactor;
        return builder;
    }

    /// <summary>
    /// Adds a topic configuration (e.g. <c>WithConfig("cleanup.policy", "compact")</c>).
    /// </summary>
    public static IResourceBuilder<KafkaTopicResource> WithConfig(
        this IResourceBuilder<KafkaTopicResource> builder,
        string key,
        string value
    )
    {
        builder.Resource.Configs[key] = value;
        return builder;
    }

    /// <summary>
    /// Configures the Kafka topic to be created automatically on AppHost startup.
    /// </summary>
    public static IResourceBuilder<KafkaTopicResource> CreateTopicOnStart(
        this IResourceBuilder<KafkaTopicResource> builder
    )
    {
        var topicResource = builder.Resource;
        topicResource.RunOnStart = true;

        builder.ApplicationBuilder.Eventing.Subscribe<ResourceReadyEvent>(
            topicResource.KafkaResource!,
            (@event, ct) => HandleKafkaReadyAsync(@event.Services, topicResource, ct)
        );

        return builder;
    }

    static async Task HandleKafkaReadyAsync(
        IServiceProvider serviceProvider,
        KafkaTopicResource topicResource,
        CancellationToken cancellationToken
    )
    {
        var resourceLoggerService = serviceProvider.GetRequiredService<ResourceLoggerService>();
        var logger = resourceLoggerService.GetLogger(topicResource);

        try
        {
            var notificationService = serviceProvider.GetRequiredService<ResourceNotificationService>();
            var result = await KafkaTopicCommandHandler.ExecuteCreateTopicAsync(
                topicResource,
                logger,
                notificationService,
                cancellationToken
            );

            if (!result.Success)
            {
                logger.LogError(
                    "Kafka topic creation on startup failed for resource '{ResourceName}'. {ErrorMessage}",
                    topicResource.Name,
                    result.Message ?? ""
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Kafka topic creation on startup failed for resource '{ResourceName}'.",
                topicResource.Name
            );
        }
    }

    static async Task<ExecuteCommandResult> HandleCommandAsync(
        ExecuteCommandContext context,
        KafkaTopicResource resource
    )
    {
        var logger = context.Logger;
        var notificationService = context.Services.GetRequiredService<ResourceNotificationService>();

        return await KafkaTopicCommandHandler.ExecuteCreateTopicAsync(
            resource,
            logger,
            notificationService,
            context.CancellationToken
        );
    }

    static ResourceCommandState GetCommandState(KafkaTopicResource resource) =>
        resource.IsExecutingCommand ? ResourceCommandState.Disabled : ResourceCommandState.Enabled;
}
