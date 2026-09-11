using System.Collections.Immutable;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Lifecycle;
using Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for adding a Debezium PostgreSQL connector resource.
/// </summary>
public static class PostgresDebeziumConnectorBuilderExtensions
{
    /// <summary>
    /// Adds a Debezium PostgreSQL connector resource under Kafka Connect.
    /// </summary>
    /// <remarks>
    /// The Debezium Postgres Source Connector uses PostgreSQL logical replication (CDC) to capture
    /// row-level changes and publish them to Kafka topics.
    /// </remarks>
    /// <related type="method" href="/Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource/PostgresDebeziumConnectorOptionsExtensions.html">WithOutboxCdc</related>
    /// <param name="builder">The Kafka Connect resource builder.</param>
    /// <param name="name">The logical resource name.</param>
    /// <param name="sourceDatabase">The source database resource.</param>
    /// <param name="configure">Optional connector configuration.</param>
    /// <returns>The resource builder for the connector resource.</returns>
    public static IResourceBuilder<PostgresDebeziumConnectorResource> AddPostgresDebeziumConnector(
        this IResourceBuilder<KafkaConnectResource> builder,
        [ResourceName] string name,
        IResourceBuilder<PostgresDatabaseResource> sourceDatabase,
        Action<PostgresDebeziumConnectorOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sourceDatabase);

        ValidateWalLevelLogical(sourceDatabase.Resource);

        builder.ApplicationBuilder.Services.TryAddEventingSubscriber<PostgresDebeziumConnectorEventingSubscriber>();
        builder.ApplicationBuilder.Services.AddSingleton<PostgresDebeziumConnectorEventingSubscriber>();

        var options = new PostgresDebeziumConnectorOptions { TopicPrefix = name };
        configure?.Invoke(options);
        options.SlotName ??= $"{options.TopicPrefix}_slot";
        options.PublicationName ??= $"{options.TopicPrefix}_publication";

        var resource = new PostgresDebeziumConnectorResource(
            name,
            builder.Resource,
            sourceDatabase.Resource,
            options
        );

        var commandOptions = new CommandOptions
        {
            Description = "Reconcile the connector definition against Kafka Connect.",
            IconName = "ArrowSync",
            UpdateState = static context =>
                context.ResourceSnapshot.HealthStatus is HealthStatus.Healthy or null
                    ? ResourceCommandState.Enabled
                    : ResourceCommandState.Disabled,
        };

        var restartCommandOptions = new CommandOptions
        {
            Description = "Restart the connector and its task.",
            IconName = "ArrowReset",
            UpdateState = commandOptions.UpdateState,
        };

        var deleteCommandOptions = new CommandOptions
        {
            Description =
                "Delete the connector from Kafka Connect and clean up related PostgreSQL objects (replication slot and publication).",
            IconName = "Delete",
            IconVariant = IconVariant.Filled,
            UpdateState = static context =>
            {
                var state = context.ResourceSnapshot.State?.Text;
                return state == KnownResourceStates.Running || state == KnownResourceStates.Starting
                    ? ResourceCommandState.Enabled
                    : ResourceCommandState.Disabled;
            },
        };

        return builder
            .ApplicationBuilder.AddResource(resource)
            .WithInitialState(
                new CustomResourceSnapshot
                {
                    ResourceType = nameof(PostgresDebeziumConnectorResource),
                    CreationTimeStamp = DateTime.UtcNow,
                    State = KnownResourceStates.NotStarted,
                    Properties = [],
                }
            )
            .WithRelationship(sourceDatabase.Resource, "captures-from")
            .WithParentRelationship(builder.Resource)
            .WithInitialState(
                new CustomResourceSnapshot
                {
                    ResourceType = nameof(PostgresDebeziumConnectorResource),
                    CreationTimeStamp = DateTime.UtcNow,
                    State = KnownResourceStates.NotStarted,
                    Properties = GetProperties(resource),
                }
            )
            .WithCommand(
                "reconcile",
                "Reconcile Connector",
                async context =>
                {
                    var subscriber =
                        context.ServiceProvider.GetRequiredService<PostgresDebeziumConnectorEventingSubscriber>();
                    return await subscriber.ReconcileCommandAsync(
                        resource,
                        context.CancellationToken
                    );
                },
                commandOptions
            )
            .WithCommand(
                "restart",
                "Restart Connector",
                async context =>
                {
                    var subscriber =
                        context.ServiceProvider.GetRequiredService<PostgresDebeziumConnectorEventingSubscriber>();
                    return await subscriber.RestartCommandAsync(
                        resource,
                        context.CancellationToken
                    );
                },
                restartCommandOptions
            )
            .WithCommand(
                "delete",
                "Delete Connector",
                async context =>
                {
                    var subscriber =
                        context.ServiceProvider.GetRequiredService<PostgresDebeziumConnectorEventingSubscriber>();
                    return await subscriber.DeleteCommandAsync(resource, context.CancellationToken);
                },
                deleteCommandOptions
            );
    }

    private static ImmutableArray<ResourcePropertySnapshot> GetProperties(
        PostgresDebeziumConnectorResource resource
    )
    {
        var properties = ImmutableArray.CreateBuilder<ResourcePropertySnapshot>();
        properties.Add(new("Topic Prefix", resource.Options.TopicPrefix));

        if (!string.IsNullOrWhiteSpace(resource.Options.TableIncludeList))
        {
            properties.Add(new("Tables", resource.Options.TableIncludeList));
        }

        properties.Add(new("Slot", resource.Options.SlotName));
        properties.Add(new("Publication", resource.Options.PublicationName));

        return properties.ToImmutable();
    }

    private static void ValidateWalLevelLogical(PostgresDatabaseResource sourceDatabase)
    {
        var parent = sourceDatabase.Parent;
        var callbackAnnotations = parent.Annotations.OfType<CommandLineArgsCallbackAnnotation>();

        var args = new List<object>();
        var context = new CommandLineArgsCallbackContext(args, CancellationToken.None);

        foreach (var annotation in callbackAnnotations)
        {
            annotation.Callback(context).GetAwaiter().GetResult();
        }

        var argStrings = args.Select(a => a?.ToString() ?? string.Empty).ToList();

        for (var i = 0; i < argStrings.Count; i++)
        {
            if (argStrings[i] == "-c" && i + 1 < argStrings.Count)
            {
                var nextArg = argStrings[i + 1];
                if (
                    nextArg.StartsWith("wal_level=", StringComparison.Ordinal)
                    && !nextArg.Equals("wal_level=logical", StringComparison.Ordinal)
                )
                {
                    throw new InvalidOperationException(
                        $"Postgres server '{parent.Name}' has wal_level set to '{nextArg.Split('=')[1]}' "
                            + $"but Debezium CDC requires 'wal_level=logical'. "
                            + $"Configure the Postgres container with: .WithArgs(\"-c\", \"wal_level=logical\")"
                    );
                }

                if (nextArg.Equals("wal_level=logical", StringComparison.Ordinal))
                {
                    return;
                }
            }
        }

        var hasWalLevelLogical = argStrings.Any(a =>
            a.ToString()!.Equals("wal_level=logical", StringComparison.Ordinal)
        );
        if (hasWalLevelLogical)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Postgres server '{parent.Name}' is missing the required 'wal_level=logical' argument. "
                + $"Debezium CDC requires logical replication to be enabled. "
                + $"Configure the Postgres container with: .WithArgs(\"-c\", \"wal_level=logical\")"
        );
    }
}
