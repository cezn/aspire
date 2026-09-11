using System.Data.Common;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Eventing;
using Aspire.Hosting.Lifecycle;
using Aspire.Hosting.Publishing;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Cezn.Aspire.Hosting.KafkaConnect.Connectors.PostgresqlSource;

/// <summary>
/// Reconciles Debezium PostgreSQL connector resources against the Kafka Connect REST API.
/// </summary>
public sealed class PostgresDebeziumConnectorEventingSubscriber(
    ResourceNotificationService notification,
    ResourceLoggerService loggerService,
    ILoggerFactory loggerFactory
) : IDistributedApplicationEventingSubscriber
{
    public Task SubscribeAsync(
        IDistributedApplicationEventing eventing,
        DistributedApplicationExecutionContext context,
        CancellationToken cancellationToken
    )
    {
        var logger = loggerFactory.CreateLogger<PostgresDebeziumConnectorEventingSubscriber>();
        logger.LogInformation("Subscribing to AfterResourcesCreatedEvent for Debezium connector reconciliation.");

        eventing.Subscribe<AfterResourcesCreatedEvent>(
            async (@event, ct) =>
            {
                if (!context.IsRunMode)
                {
                    logger.LogInformation("Skipping Debezium connector reconciliation in publish mode.");
                    return;
                }

                var connectors = @event.Model.Resources.OfType<PostgresDebeziumConnectorResource>().ToList();

                // Delay to ensure dashboard reflects state changes.
                await Task.Delay(2000, ct);

                logger.LogInformation(
                    "Found {ConnectorCount} PostgresDebeziumConnectorResource(s) to reconcile.",
                    connectors.Count
                );

                foreach (var resource in connectors)
                {
                    logger.LogInformation("Starting reconciliation for connector {ConnectorName}.", resource.Name);
                    try
                    {
                        await ReconcileAsync(resource, ct);
                        logger.LogInformation("Successfully reconciled connector {ConnectorName}.", resource.Name);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to reconcile connector {ConnectorName}.", resource.Name);

                        await notification.PublishUpdateAsync(
                            resource,
                            snapshot => snapshot with { State = KnownResourceStates.FailedToStart }
                        );
                    }
                }
            }
        );

        eventing.Subscribe<AfterPublishEvent>(
            async (@event, ct) =>
            {
                logger.LogInformation("AfterPublishEvent fired. Generating connector creation scripts.");

                var connectors = @event.Model.Resources.OfType<PostgresDebeziumConnectorResource>().ToList();

                if (connectors.Count == 0)
                {
                    logger.LogInformation("No Debezium connector resources found. Skipping script generation.");
                    return;
                }

                logger.LogInformation(
                    "Found {ConnectorCount} PostgresDebeziumConnectorResource(s) for script generation.",
                    connectors.Count
                );

                try
                {
                    await GenerateConnectorScriptsAsync(connectors, logger, ct);
                    logger.LogInformation("Connector creation scripts generated successfully.");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to generate connector creation scripts.");
                }
            }
        );

        return Task.CompletedTask;
    }

    public async Task<ExecuteCommandResult> ReconcileCommandAsync(
        PostgresDebeziumConnectorResource resource,
        CancellationToken cancellationToken
    )
    {
        var logger = loggerService.GetLogger(resource);
        logger.LogInformation("Reconcile command invoked for connector {ConnectorName}.", resource.Name);

        try
        {
            await ReconcileAsync(resource, cancellationToken);
            logger.LogInformation("Reconcile command succeeded for connector {ConnectorName}.", resource.Name);
            return CommandResults.Success();
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Reconcile command was canceled for connector {ConnectorName}.", resource.Name);
            return CommandResults.Canceled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Reconcile command failed for connector {ConnectorName}.", resource.Name);
            return CommandResults.Failure(ex);
        }
    }

    public async Task<ExecuteCommandResult> RestartCommandAsync(
        PostgresDebeziumConnectorResource resource,
        CancellationToken cancellationToken
    )
    {
        var logger = loggerService.GetLogger(resource);
        logger.LogInformation("Restart command invoked for connector {ConnectorName}.", resource.Name);

        try
        {
            var connectBaseUri = await GetKafkaConnectBaseUriAsync(resource, logger, cancellationToken);
            logger.LogInformation("Using Kafka Connect base URI: {ConnectBaseUri}.", connectBaseUri);

            using var client = CreateHttpClient(connectBaseUri);
            logger.LogInformation("Sending POST restart request for connector {ConnectorName}.", resource.Name);

            var response = await client.PostAsync(
                $"connectors/{resource.Name}/restart?includeTasks=true",
                null,
                cancellationToken
            );

            if (!response.IsSuccessStatusCode)
            {
                var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
                logger.LogError(
                    "Restart request failed for connector {ConnectorName}. Status {StatusCode}: {ResponseBody}.",
                    resource.Name,
                    (int)response.StatusCode,
                    responseBody
                );
                throw new InvalidOperationException(
                    $"Failed to restart connector '{resource.Name}'. Status {(int)response.StatusCode}: {responseBody}"
                );
            }

            logger.LogInformation("Requested restart for connector {ConnectorName}.", resource.Name);
            await WaitForConnectorRunningAsync(client, resource, logger, cancellationToken);

            logger.LogInformation("Restart command succeeded for connector {ConnectorName}.", resource.Name);
            return CommandResults.Success();
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Restart command was canceled for connector {ConnectorName}.", resource.Name);
            return CommandResults.Canceled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Restart command failed for connector {ConnectorName}.", resource.Name);
            return CommandResults.Failure(ex);
        }
    }

    public async Task<ExecuteCommandResult> DeleteCommandAsync(
        PostgresDebeziumConnectorResource resource,
        CancellationToken cancellationToken
    )
    {
        var logger = loggerService.GetLogger(resource);
        logger.LogInformation("Delete command invoked for connector {ConnectorName}.", resource.Name);

        try
        {
            // 1. Delete the connector from Kafka Connect
            await DeleteConnectorFromKafkaConnectAsync(resource, logger, cancellationToken);

            // 2. Clean up PostgreSQL replication slot and publication
            await CleanupPostgresObjectsAsync(resource, logger, cancellationToken);

            // 3. Update resource state
            await notification.PublishUpdateAsync(resource, snapshot => snapshot with { State = "Deleted" });

            logger.LogInformation("Delete command succeeded for connector {ConnectorName}.", resource.Name);
            return CommandResults.Success();
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Delete command was canceled for connector {ConnectorName}.", resource.Name);
            return CommandResults.Canceled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delete command failed for connector {ConnectorName}.", resource.Name);
            return CommandResults.Failure(ex);
        }
    }

    private async Task DeleteConnectorFromKafkaConnectAsync(
        PostgresDebeziumConnectorResource resource,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        logger.LogInformation(
            "Waiting for Kafka Connect resource {ResourceName} to become healthy.",
            resource.Parent.Name
        );
        await notification.WaitForResourceHealthyAsync(resource.Parent.Name, cancellationToken);

        var connectBaseUri = await GetKafkaConnectBaseUriAsync(resource, logger, cancellationToken);
        logger.LogInformation(
            "Deleting connector {ConnectorName} from Kafka Connect at {ConnectBaseUri}.",
            resource.Name,
            connectBaseUri
        );

        using var client = CreateHttpClient(connectBaseUri);
        var response = await client.DeleteAsync($"connectors/{resource.Name}", cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            logger.LogError(
                "Failed to delete connector {ConnectorName}. Status {StatusCode}: {ResponseBody}.",
                resource.Name,
                (int)response.StatusCode,
                responseBody
            );
            throw new InvalidOperationException(
                $"Failed to delete connector '{resource.Name}' from Kafka Connect. Status {(int)response.StatusCode}: {responseBody}"
            );
        }

        logger.LogInformation("Successfully deleted connector {ConnectorName} from Kafka Connect.", resource.Name);
    }

    private async Task CleanupPostgresObjectsAsync(
        PostgresDebeziumConnectorResource resource,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        var connectionString =
            await ((IResourceWithConnectionString)resource.SourceDatabase).GetConnectionStringAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                $"Connection string for source database '{resource.SourceDatabase.Name}' is not available."
            );

        logger.LogInformation(
            "Cleaning up replication slot '{SlotName}' and publication '{PublicationName}' on database {DatabaseName}.",
            resource.Options.SlotName,
            resource.Options.PublicationName,
            resource.SourceDatabase.Name
        );

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // Drop replication slot (IF EXISTS is not supported for pg_drop_replication_slot, so we check first)
        var slotExists = await CheckReplicationSlotExistsAsync(
            connection,
            resource.Options.SlotName,
            cancellationToken
        );
        if (slotExists)
        {
            // Retry dropping the slot — it may still be active in Postgres after the connector is deleted
            const int maxAttempts = 10;
            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                await using var dropSlotCmd = connection.CreateCommand();
                dropSlotCmd.CommandText = $"SELECT pg_drop_replication_slot('{resource.Options.SlotName}')";

                try
                {
                    await dropSlotCmd.ExecuteNonQueryAsync(cancellationToken);
                    logger.LogInformation("Dropped replication slot '{SlotName}'.", resource.Options.SlotName);
                    break;
                }
                catch (PostgresException ex) when (ex.SqlState == "55006" && attempt < maxAttempts)
                {
                    logger.LogWarning(
                        "Replication slot '{SlotName}' is still active (attempt {Attempt}/{MaxAttempts}). Retrying in 1s.",
                        resource.Options.SlotName,
                        attempt,
                        maxAttempts
                    );
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
            }
        }
        else
        {
            logger.LogInformation("Replication slot '{SlotName}' does not exist, skipping.", resource.Options.SlotName);
        }

        // Drop publication
        await using var dropPubCmd = connection.CreateCommand();
        dropPubCmd.CommandText = $"DROP PUBLICATION IF EXISTS {resource.Options.PublicationName}";
        await dropPubCmd.ExecuteNonQueryAsync(cancellationToken);
        logger.LogInformation("Dropped publication '{PublicationName}'.", resource.Options.PublicationName);
    }

    private static async Task<bool> CheckReplicationSlotExistsAsync(
        NpgsqlConnection connection,
        string slotName,
        CancellationToken cancellationToken
    )
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM pg_replication_slots WHERE slot_name = @slotName";
        cmd.Parameters.AddWithValue("@slotName", slotName);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is not null;
    }

    private async Task ReconcileAsync(PostgresDebeziumConnectorResource resource, CancellationToken cancellationToken)
    {
        var logger = loggerService.GetLogger(resource);
        logger.LogInformation("ReconcileAsync started for connector {ConnectorName}.", resource.Name);

        logger.LogInformation(
            "Waiting for Kafka Connect resource {ResourceName} to become healthy.",
            resource.Parent.Name
        );
        await notification.WaitForResourceHealthyAsync(resource.Parent.Name, cancellationToken);

        await notification.PublishUpdateAsync(
            resource,
            snapshot => snapshot with { State = KnownResourceStates.Starting }
        );

        logger.LogInformation("Kafka Connect resource {ResourceName} is healthy.", resource.Parent.Name);

        var connectBaseUri = await GetKafkaConnectBaseUriAsync(resource, logger, cancellationToken);
        logger.LogInformation(
            "Reconciling Debezium connector {ConnectorName} against {ConnectBaseUri}.",
            resource.Name,
            connectBaseUri
        );

        using var client = CreateHttpClient(connectBaseUri);

        logger.LogInformation("Checking Debezium plugin availability for connector {ConnectorName}.", resource.Name);
        await EnsurePluginInstalledAsync(client, resource.Name, logger, cancellationToken);
        logger.LogInformation("Debezium plugin verified for connector {ConnectorName}.", resource.Name);

        logger.LogInformation("Building connector payload for {ConnectorName}.", resource.Name);
        var payload = await BuildConnectorPayloadAsync(resource, logger, cancellationToken);
        logger.LogInformation(
            "Connector payload built for {ConnectorName} with {ConfigCount} settings.",
            resource.Name,
            payload.Count
        );

        logger.LogInformation("Sending PUT request to create/update connector {ConnectorName}.", resource.Name);
        var response = await client.PutAsJsonAsync($"connectors/{resource.Name}/config", payload, cancellationToken);

        logger.LogInformation(
            "Connector create/update request returned status {StatusCode} for {ConnectorName}.",
            (int)response.StatusCode,
            resource.Name
        );

        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            logger.LogError(
                "Failed to create/update connector {ConnectorName}. Status {StatusCode}: {ResponseBody}.",
                resource.Name,
                (int)response.StatusCode,
                responseBody
            );

            await notification.PublishUpdateAsync(
                resource,
                snapshot => snapshot with { State = KnownResourceStates.FailedToStart }
            );

            throw new InvalidOperationException(
                $"Failed to reconcile connector '{resource.Name}'. Status {(int)response.StatusCode}: {responseBody}"
            );
        }

        logger.LogInformation("Waiting for connector {ConnectorName} to reach RUNNING state.", resource.Name);
        await WaitForConnectorRunningAsync(client, resource, logger, cancellationToken);
        logger.LogInformation("Connector {ConnectorName} is now RUNNING.", resource.Name);

        await notification.PublishUpdateAsync(
            resource,
            snapshot => snapshot with { State = KnownResourceStates.Running }
        );
    }

    private static HttpClient CreateHttpClient(string connectBaseUri) =>
        new() { BaseAddress = new Uri(connectBaseUri, UriKind.Absolute), Timeout = TimeSpan.FromSeconds(15) };

    private static async Task<string> GetKafkaConnectBaseUriAsync(
        PostgresDebeziumConnectorResource resource,
        ILogger? logger,
        CancellationToken cancellationToken
    )
    {
        var endpoint = resource.Parent.GetEndpoint(
            KafkaConnectResource.HttpEndpointName,
            KnownNetworkIdentifiers.LocalhostNetwork
        );

        var hostAndPort = await endpoint.Property(EndpointProperty.HostAndPort).GetValueAsync(cancellationToken);

        var baseUri = $"http://{hostAndPort}/";
        logger?.LogDebug(
            "Resolved Kafka Connect base URI: {BaseUri} for resource {ResourceName}.",
            baseUri,
            resource.Parent.Name
        );
        return baseUri;
    }

    private static async Task EnsurePluginInstalledAsync(
        HttpClient client,
        string connectorName,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        for (var attempt = 0; attempt < 30; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                logger.LogDebug(
                    "Checking connector plugins (attempt {Attempt}/30) for {ConnectorName}.",
                    attempt + 1,
                    connectorName
                );

                var plugins =
                    await client.GetFromJsonAsync<JsonArray>("connector-plugins", cancellationToken)
                    ?? throw new InvalidOperationException("Kafka Connect did not return any connector plugins.");

                logger.LogDebug("Kafka Connect returned {PluginCount} connector plugin(s).", plugins.Count);

                var pluginExists = plugins
                    .OfType<JsonObject>()
                    .Select(p => p["class"]?.GetValue<string>())
                    .Any(c => c == "io.debezium.connector.postgresql.PostgresConnector");

                if (!pluginExists)
                    throw new InvalidOperationException(
                        "Kafka Connect does not have the Debezium PostgreSQL connector installed."
                    );

                logger.LogInformation(
                    "Debezium PostgreSQL plugin found on attempt {Attempt} for {ConnectorName}.",
                    attempt + 1,
                    connectorName
                );
                return;
            }
            catch (HttpRequestException ex) when (attempt < 29)
            {
                logger.LogDebug(
                    "HttpRequestException on attempt {Attempt}/30 for {ConnectorName}. Retrying in 1s. Error: {Message}.",
                    attempt + 1,
                    connectorName,
                    ex.Message
                );
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
            catch (InvalidOperationException ex) when (attempt < 29)
            {
                logger.LogDebug(
                    "InvalidOperationException on attempt {Attempt}/30 for {ConnectorName}. Retrying in 1s. Error: {Message}.",
                    attempt + 1,
                    connectorName,
                    ex.Message
                );
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }

        logger.LogError(
            "Timed out waiting for Kafka Connect REST API or Debezium plugin for {ConnectorName} after 30 attempts.",
            connectorName
        );
        throw new TimeoutException("Kafka Connect REST API did not become ready before the timeout elapsed.");
    }

    private static async Task<Dictionary<string, string>> BuildConnectorPayloadAsync(
        PostgresDebeziumConnectorResource resource,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("Building connector payload for {ConnectorName}.", resource.Name);

        var connectionString =
            await ((IResourceWithConnectionString)resource.SourceDatabase).GetConnectionStringAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                $"Connection string for source database '{resource.SourceDatabase.Name}' is not available."
            );

        logger.LogDebug("Got connection string for source database {DatabaseName}.", resource.SourceDatabase.Name);

        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

        var user = GetRequiredConnectionValue(builder, ["Username", "User ID", "UserID", "User Name"]);
        var password = GetRequiredConnectionValue(builder, ["Password"]);
        var databaseName = GetRequiredConnectionValue(builder, ["Database", "Initial Catalog"]);

        logger.LogDebug("Parsed connection details: user={User}, database={Database}.", user, databaseName);

        var containerNetworkContext = new ValueProviderContext
        {
            Network = KnownNetworkIdentifiers.DefaultAspireContainerNetwork,
        };

        var host =
            await resource
                .SourceDatabase.Parent.PrimaryEndpoint.Property(EndpointProperty.Host)
                .GetValueAsync(containerNetworkContext, cancellationToken)
            ?? throw new InvalidOperationException("Postgres internal host is not available.");

        var port =
            await resource
                .SourceDatabase.Parent.PrimaryEndpoint.Property(EndpointProperty.Port)
                .GetValueAsync(containerNetworkContext, cancellationToken)
            ?? throw new InvalidOperationException("Postgres internal port is not available.");

        logger.LogDebug("Resolved Postgres endpoint: host={Host}, port={Port}.", host, port);

        var payload = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = "io.debezium.connector.postgresql.PostgresConnector",
            ["tasks.max"] = "1",
            ["database.hostname"] = host,
            ["database.port"] = port,
            ["database.user"] = user,
            ["database.password"] = password,
            ["database.dbname"] = databaseName,
            ["plugin.name"] = "pgoutput",
            ["topic.prefix"] = resource.Options.TopicPrefix,
            ["slot.name"] = resource.Options.SlotName,
            ["publication.name"] = resource.Options.PublicationName,
            ["publication.autocreate.mode"] = "filtered",
            ["snapshot.mode"] = resource.Options.SnapshotMode,
            ["heartbeat.interval.ms"] = resource.Options.HeartbeatIntervalMs.ToString(),
            ["provide.transaction.metadata"] = resource
                .Options.ProvideTransactionMetadata.ToString()
                .ToLowerInvariant(),
            ["tombstones.on.delete"] = "true",
            ["key.converter"] = string.IsNullOrWhiteSpace(resource.Options.KeyConverter)
                ? "org.apache.kafka.connect.json.JsonConverter"
                : resource.Options.KeyConverter,
            // ["key.converter.schemas.enable"] = "false",
            ["value.converter"] = string.IsNullOrWhiteSpace(resource.Options.ValueConverter)
                ? "org.apache.kafka.connect.json.JsonConverter"
                : resource.Options.ValueConverter,
            // ["value.converter.schemas.enable"] = "false",
        };

        if (!string.IsNullOrWhiteSpace(resource.Options.SchemaIncludeList))
            payload["schema.include.list"] = resource.Options.SchemaIncludeList;

        if (!string.IsNullOrWhiteSpace(resource.Options.TableIncludeList))
            payload["table.include.list"] = resource.Options.TableIncludeList;

        if (resource.Options.KeyConverterSchemaRegistryUrl is not null)
            payload["key.converter.schema.registry.url"] =
                await resource
                    .Options.KeyConverterSchemaRegistryUrl.Property(EndpointProperty.Url)
                    .GetValueAsync(
                        new ValueProviderContext { Network = KnownNetworkIdentifiers.DefaultAspireContainerNetwork },
                        cancellationToken
                    )
                ?? throw new InvalidOperationException("Key converter schema registry URL is not available.");

        if (!string.IsNullOrWhiteSpace(resource.Options.ValueConverterSchemaRegistryUrl))
            payload["value.converter.schema.registry.url"] = resource.Options.ValueConverterSchemaRegistryUrl;

        if (!string.IsNullOrWhiteSpace(resource.Options.ValueConverterDelegateConverterType))
            payload["value.converter.delegate.converter.type"] = resource.Options.ValueConverterDelegateConverterType;

        if (resource.Options.ValueConverterDelegateConverterSchemasEnable.HasValue)
            payload["value.converter.delegate.converter.type.schemas.enable"] = resource
                .Options.ValueConverterDelegateConverterSchemasEnable.Value.ToString()
                .ToLowerInvariant();

        if (!string.IsNullOrWhiteSpace(resource.Options.Transforms))
            payload["transforms"] = resource.Options.Transforms;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteType))
            payload["transforms.route.type"] = resource.Options.TransformsRouteType;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteRegex))
            payload["transforms.route.regex"] = resource.Options.TransformsRouteRegex;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteReplacement))
            payload["transforms.route.replacement"] = resource.Options.TransformsRouteReplacement;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxType))
            payload["transforms.outbox.type"] = resource.Options.TransformsOutboxType;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxTableFieldEventKey))
            payload["transforms.outbox.table.field.event.key"] = resource.Options.TransformsOutboxTableFieldEventKey;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxTableFieldsAdditionalPlacement))
            payload["transforms.outbox.table.fields.additional.placement"] = resource
                .Options
                .TransformsOutboxTableFieldsAdditionalPlacement;

        if (!string.IsNullOrWhiteSpace(resource.Options.TracingSpanContextField))
            payload["tracing.span.context.field"] = resource.Options.TracingSpanContextField;

        if (resource.Options.TracingWithContextFieldOnly.HasValue)
            payload["tracing.with.context.field.only"] = resource
                .Options.TracingWithContextFieldOnly.Value.ToString()
                .ToLowerInvariant();

        // Merge arbitrary connector config (lowest priority — strongly-typed options take precedence)
        foreach (var (key, value) in resource.Options.ConnectorConfig)
        {
            if (!payload.ContainsKey(key))
                payload[key] = value;
        }

        logger.LogDebug(
            "Final payload for {ConnectorName} contains {ConfigCount} key-value pairs.",
            resource.Name,
            payload.Count
        );
        return payload;
    }

    private static string GetRequiredConnectionValue(DbConnectionStringBuilder builder, string[] keys)
    {
        foreach (var key in keys)
        {
            if (
                builder.TryGetValue(key, out var value)
                && value is string stringValue
                && !string.IsNullOrWhiteSpace(stringValue)
            )
            {
                return stringValue;
            }
        }

        throw new InvalidOperationException(
            $"Connection string is missing one of the required keys: {string.Join(", ", keys)}."
        );
    }

    private async Task WaitForConnectorRunningAsync(
        HttpClient client,
        PostgresDebeziumConnectorResource resource,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        logger.LogInformation("Polling connector {ConnectorName} status (up to 20 attempts).", resource.Name);

        for (var attempt = 0; attempt < 20; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await client.GetAsync($"connectors/{resource.Name}/status", cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                logger.LogDebug(
                    "Status check returned {StatusCode} for {ConnectorName} (attempt {Attempt}/20). Retrying in 1s.",
                    (int)response.StatusCode,
                    resource.Name,
                    attempt + 1
                );
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                continue;
            }

            var status =
                await response.Content.ReadFromJsonAsync<JsonObject>(cancellationToken)
                ?? throw new InvalidOperationException(
                    $"Kafka Connect returned an empty status payload for connector '{resource.Name}'."
                );

            var connectorState = status["connector"]?["state"]?.GetValue<string>();
            var taskStates =
                status["tasks"]
                    ?.AsArray()
                    .OfType<JsonObject>()
                    .Select(t => t["state"]?.GetValue<string>())
                    .Where(s => s is not null)
                    .ToArray()
                ?? [];

            logger.LogDebug(
                "Connector {ConnectorName} status (attempt {Attempt}/20): connectorState={State}, taskCount={TaskCount}.",
                resource.Name,
                attempt + 1,
                connectorState ?? "null",
                taskStates.Length
            );

            if (
                string.Equals(connectorState, "FAILED", StringComparison.OrdinalIgnoreCase)
                || taskStates.Any(s => string.Equals(s, "FAILED", StringComparison.OrdinalIgnoreCase))
            )
            {
                var trace = status["tasks"]
                    ?.AsArray()
                    .OfType<JsonObject>()
                    .Select(t => t["trace"]?.GetValue<string>())
                    .FirstOrDefault(static v => !string.IsNullOrWhiteSpace(v));

                var message = string.IsNullOrWhiteSpace(trace)
                    ? $"Kafka Connect reported FAILED for connector '{resource.Name}'."
                    : $"Kafka Connect reported FAILED for connector '{resource.Name}': {trace}";

                logger.LogError(
                    "Connector {ConnectorName} has entered FAILED state: {Message}.",
                    resource.Name,
                    message
                );

                await notification.PublishUpdateAsync(
                    resource,
                    snapshot => snapshot with { State = KnownResourceStates.FailedToStart }
                );

                throw new InvalidOperationException(message);
            }

            if (
                string.Equals(connectorState, "RUNNING", StringComparison.OrdinalIgnoreCase)
                && taskStates.Length > 0
                && taskStates.All(s => string.Equals(s, "RUNNING", StringComparison.OrdinalIgnoreCase))
            )
            {
                logger.LogInformation(
                    "Connector {ConnectorName} reached RUNNING state after {Attempt} attempt(s).",
                    resource.Name,
                    attempt + 1
                );
                return;
            }

            logger.LogDebug("Connector {ConnectorName} not yet RUNNING. Waiting 1s before next poll.", resource.Name);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        logger.LogError("Connector {ConnectorName} did not reach RUNNING state after 20 attempts.", resource.Name);
        throw new TimeoutException(
            $"Connector '{resource.Name}' did not reach RUNNING state before the timeout elapsed."
        );
    }

    private async Task GenerateConnectorScriptsAsync(
        IReadOnlyList<PostgresDebeziumConnectorResource> connectors,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        // Determine output directory: aspire-output relative to the AppHost directory
        var outputDirectory = ResolvePublishOutputDirectory();
        var scriptsDirectory = Path.Combine(outputDirectory, "connectors");
        Directory.CreateDirectory(scriptsDirectory);

        logger.LogInformation("Connector scripts will be written to {ScriptsDirectory}.", scriptsDirectory);

        // Generate individual scripts per connector
        foreach (var resource in connectors)
        {
            try
            {
                var payload = BuildConnectorPayloadForPublish(resource, logger);
                var scriptContent = BuildConnectorScript(resource, payload);
                var scriptPath = Path.Combine(scriptsDirectory, $"{resource.Name}.sh");

                await File.WriteAllTextAsync(scriptPath, scriptContent, cancellationToken);
                logger.LogInformation(
                    "Generated connector script for {ConnectorName} at {ScriptPath}.",
                    resource.Name,
                    scriptPath
                );
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to generate connector script for {ConnectorName}.", resource.Name);
            }
        }

        // Generate a master script that runs all connector scripts
        var masterScriptPath = Path.Combine(scriptsDirectory, "create-all-connectors.sh");
        var masterScriptContent = BuildMasterScript(connectors);
        await File.WriteAllTextAsync(masterScriptPath, masterScriptContent, cancellationToken);

        // Make scripts executable (on Unix-like systems)
        try
        {
            foreach (var resource in connectors)
            {
                var scriptPath = Path.Combine(scriptsDirectory, $"{resource.Name}.sh");
                var process = new System.Diagnostics.Process
                {
                    StartInfo = new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = "chmod",
                        Arguments = $"+x \"{scriptPath}\"",
                        UseShellExecute = false,
                        CreateNoWindow = true,
                    },
                };
                process.Start();
            }
            var masterProcess = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "chmod",
                    Arguments = $"+x \"{masterScriptPath}\"",
                    UseShellExecute = false,
                    CreateNoWindow = true,
                },
            };
            masterProcess.Start();
        }
        catch
        {
            // chmod may not be available on Windows; that's fine
        }

        logger.LogInformation("Master connector script generated at {MasterScriptPath}.", masterScriptPath);
    }

    private static string ResolvePublishOutputDirectory()
    {
        // Try to resolve the AppHost directory from the current directory structure
        // The AppHost typically runs from its own directory, and aspire-output is created there
        var currentDirectory = Environment.CurrentDirectory;
        var outputDirectory = Path.Combine(currentDirectory, "aspire-output");

        // If aspire-output doesn't exist, try parent directories
        if (!Directory.Exists(outputDirectory))
        {
            var directory = currentDirectory;
            var maxDepth = 5;
            for (var i = 0; i < maxDepth; i++)
            {
                var parentOutput = Path.Combine(directory, "aspire-output");
                if (Directory.Exists(parentOutput))
                {
                    outputDirectory = parentOutput;
                    break;
                }
                var parent = Path.GetDirectoryName(directory);
                if (parent == null)
                {
                    break;
                }
                directory = parent;
            }
        }

        Directory.CreateDirectory(outputDirectory);
        return outputDirectory;
    }

    private static Dictionary<string, string> BuildConnectorPayloadForPublish(
        PostgresDebeziumConnectorResource resource,
        ILogger logger
    )
    {
        logger.LogDebug("Building connector payload for publish mode: {ConnectorName}.", resource.Name);

        // During publish mode, all values are templates/placeholders resolved at deploy time.
        // Use static data from the resource model — no async resolution needed.

        var postgresServer = resource.SourceDatabase.Parent;
        var postgresServerName = postgresServer.Name;

        // Resolve port from endpoint annotation (static, no async needed)
        var postgresPort = "5432";
        if (postgresServer.PrimaryEndpoint.EndpointAnnotation.Port is int explicitPort)
        {
            postgresPort = explicitPort.ToString();
        }

        // User and password are from parameters — use placeholders
        var user = postgresServer.UserNameParameter is not null
            ? $"__{postgresServer.UserNameParameter.Name.ToUpperInvariant()}__"
            : "postgres";
        var password = $"__{postgresServer.PasswordParameter.Name.ToUpperInvariant()}__";

        var payload = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = "io.debezium.connector.postgresql.PostgresConnector",
            ["tasks.max"] = "1",
            ["database.hostname"] = postgresServerName,
            ["database.port"] = postgresPort,
            ["database.user"] = user,
            ["database.password"] = password,
            ["database.dbname"] = resource.SourceDatabase.DatabaseName,
            ["plugin.name"] = "pgoutput",
            ["topic.prefix"] = resource.Options.TopicPrefix,
            ["slot.name"] = resource.Options.SlotName,
            ["publication.name"] = resource.Options.PublicationName,
            ["publication.autocreate.mode"] = "filtered",
            ["snapshot.mode"] = resource.Options.SnapshotMode,
            ["heartbeat.interval.ms"] = resource.Options.HeartbeatIntervalMs.ToString(),
            ["provide.transaction.metadata"] = resource
                .Options.ProvideTransactionMetadata.ToString()
                .ToLowerInvariant(),
            ["tombstones.on.delete"] = "true",
            ["key.converter"] = string.IsNullOrWhiteSpace(resource.Options.KeyConverter)
                ? "org.apache.kafka.connect.json.JsonConverter"
                : resource.Options.KeyConverter,
            ["value.converter"] = string.IsNullOrWhiteSpace(resource.Options.ValueConverter)
                ? "org.apache.kafka.connect.json.JsonConverter"
                : resource.Options.ValueConverter,
        };

        if (!string.IsNullOrWhiteSpace(resource.Options.SchemaIncludeList))
            payload["schema.include.list"] = resource.Options.SchemaIncludeList;

        if (!string.IsNullOrWhiteSpace(resource.Options.TableIncludeList))
            payload["table.include.list"] = resource.Options.TableIncludeList;

        // Schema Registry URL — use placeholder resolved at runtime via env var
        if (resource.Options.KeyConverterSchemaRegistryUrl is not null)
        {
            payload["key.converter.schema.registry.url"] = "__SCHEMA_REGISTRY_URL__";
        }

        if (!string.IsNullOrWhiteSpace(resource.Options.ValueConverterSchemaRegistryUrl))
            payload["value.converter.schema.registry.url"] = resource.Options.ValueConverterSchemaRegistryUrl;

        if (!string.IsNullOrWhiteSpace(resource.Options.ValueConverterDelegateConverterType))
            payload["value.converter.delegate.converter.type"] = resource.Options.ValueConverterDelegateConverterType;

        if (resource.Options.ValueConverterDelegateConverterSchemasEnable.HasValue)
            payload["value.converter.delegate.converter.type.schemas.enable"] = resource
                .Options.ValueConverterDelegateConverterSchemasEnable.Value.ToString()
                .ToLowerInvariant();

        if (!string.IsNullOrWhiteSpace(resource.Options.Transforms))
            payload["transforms"] = resource.Options.Transforms;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteType))
            payload["transforms.route.type"] = resource.Options.TransformsRouteType;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteRegex))
            payload["transforms.route.regex"] = resource.Options.TransformsRouteRegex;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsRouteReplacement))
            payload["transforms.route.replacement"] = resource.Options.TransformsRouteReplacement;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxType))
            payload["transforms.outbox.type"] = resource.Options.TransformsOutboxType;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxTableFieldEventKey))
            payload["transforms.outbox.table.field.event.key"] = resource.Options.TransformsOutboxTableFieldEventKey;

        if (!string.IsNullOrWhiteSpace(resource.Options.TransformsOutboxTableFieldsAdditionalPlacement))
            payload["transforms.outbox.table.fields.additional.placement"] = resource
                .Options
                .TransformsOutboxTableFieldsAdditionalPlacement;

        if (!string.IsNullOrWhiteSpace(resource.Options.TracingSpanContextField))
            payload["tracing.span.context.field"] = resource.Options.TracingSpanContextField;

        if (resource.Options.TracingWithContextFieldOnly.HasValue)
            payload["tracing.with.context.field.only"] = resource
                .Options.TracingWithContextFieldOnly.Value.ToString()
                .ToLowerInvariant();

        // Merge arbitrary connector config (lowest priority — strongly-typed options take precedence)
        foreach (var (key, value) in resource.Options.ConnectorConfig)
        {
            if (!payload.ContainsKey(key))
                payload[key] = value;
        }

        logger.LogDebug(
            "Publish-mode payload for {ConnectorName} contains {ConfigCount} key-value pairs.",
            resource.Name,
            payload.Count
        );
        return payload;
    }

    private static string BuildConnectorScript(
        PostgresDebeziumConnectorResource resource,
        Dictionary<string, string> payload
    )
    {
        var sb = new StringBuilder();

        sb.AppendLine("#!/bin/bash");
        sb.AppendLine("#");
        sb.AppendLine($"# Kafka Connect connector creation script for '{resource.Name}'");
        sb.AppendLine($"# Generated by Aspire AppHost during publish");
        sb.AppendLine($"#");
        sb.AppendLine("# Usage:");
        sb.AppendLine("#   ./create-all-connectors.sh          # Run all connectors");
        sb.AppendLine("#   KAFKA_CONNECT_URL=http://localhost:8083 ./create-all-connectors.sh  # External URL");
        sb.AppendLine(
            "#   SCHEMA_REGISTRY_URL=http://localhost:8081 ./create-all-connectors.sh  # External Schema Registry"
        );
        sb.AppendLine("#");
        sb.AppendLine();

        sb.AppendLine("set -euo pipefail");
        sb.AppendLine();
        sb.AppendLine("# Kafka Connect URL - defaults to Docker Compose internal service");
        sb.AppendLine("KAFKA_CONNECT_URL=\"${KAFKA_CONNECT_URL:-http://kafka-connect:8083}\"");
        sb.AppendLine();
        sb.AppendLine("# Schema Registry URL - defaults to Docker Compose internal service");
        sb.AppendLine("SCHEMA_REGISTRY_URL=\"${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}\"");
        sb.AppendLine();
        sb.AppendLine("echo \"Creating connector '" + resource.Name + "' against $KAFKA_CONNECT_URL ...\"");
        sb.AppendLine();

        // Wait for Kafka Connect to be ready
        sb.AppendLine("# Wait for Kafka Connect to be ready");
        sb.AppendLine("MAX_RETRIES=30");
        sb.AppendLine("RETRY_COUNT=0");
        sb.AppendLine("while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do");
        sb.AppendLine("    if curl -sf \"$KAFKA_CONNECT_URL/connector-plugins\" > /dev/null 2>&1; then");
        sb.AppendLine("        echo \"Kafka Connect is ready.\"");
        sb.AppendLine("        break");
        sb.AppendLine("    fi");
        sb.AppendLine("    RETRY_COUNT=$((RETRY_COUNT + 1))");
        sb.AppendLine("    echo \"Waiting for Kafka Connect... (attempt $RETRY_COUNT/$MAX_RETRIES)\"");
        sb.AppendLine("    sleep 2");
        sb.AppendLine("done");
        sb.AppendLine();
        sb.AppendLine("if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then");
        sb.AppendLine("    echo \"ERROR: Kafka Connect did not become ready after $MAX_RETRIES attempts.\"");
        sb.AppendLine("    exit 1");
        sb.AppendLine("fi");
        sb.AppendLine();

        // Generate the JSON payload (may contain __SCHEMA_REGISTRY_URL__ placeholder)
        var jsonPayload = JsonSerializer.Serialize(
            payload,
            new JsonSerializerOptions { WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
        );

        // Escape single quotes in the JSON for the heredoc
        var escapedJson = jsonPayload.Replace("'", "'\\''");

        sb.AppendLine("# Connector configuration (template with placeholders resolved at runtime)");
        sb.AppendLine("CONNECTOR_CONFIG_TEMPLATE='");
        sb.AppendLine(escapedJson);
        sb.AppendLine("'");
        sb.AppendLine();
        sb.AppendLine("# Resolve placeholders with environment variables");
        sb.AppendLine(
            "CONNECTOR_CONFIG=$(echo \"$CONNECTOR_CONFIG_TEMPLATE\" | sed \"s|__SCHEMA_REGISTRY_URL__|$SCHEMA_REGISTRY_URL|g\")"
        );
        sb.AppendLine(
            "# Resolve remaining __VAR_NAME__ placeholders from environment (e.g. __PG_PASSWORD__ -> $PG_PASSWORD)"
        );
        sb.AppendLine(
            "CONNECTOR_CONFIG=$(echo \"$CONNECTOR_CONFIG\" | sed -E 's/__([A-Z_]+)__/${\\1}/g')"
        );
        sb.AppendLine();

        // Curl command to create the connector
        sb.AppendLine("# Create or update the connector");
        sb.AppendLine("RESPONSE=$(curl -s -w \"\\n%{http_code}\" -X PUT \\");
        sb.AppendLine("    -H \"Content-Type: application/json\" \\");
        sb.AppendLine("    -d \"$CONNECTOR_CONFIG\" \\");
        sb.AppendLine($"    \"$KAFKA_CONNECT_URL/connectors/{resource.Name}/config\")");
        sb.AppendLine();
        sb.AppendLine("HTTP_CODE=$(echo \"$RESPONSE\" | tail -n1)");
        sb.AppendLine("BODY=$(echo \"$RESPONSE\" | sed '$d')");
        sb.AppendLine();
        sb.AppendLine("if [ \"$HTTP_CODE\" -ge 200 ] && [ \"$HTTP_CODE\" -lt 300 ]; then");
        sb.AppendLine($"    echo \"SUCCESS: Connector '{resource.Name}' created/updated.\"");
        sb.AppendLine("else");
        sb.AppendLine($"    echo \"ERROR: Failed to create connector '{resource.Name}'. HTTP $HTTP_CODE\"");
        sb.AppendLine("    echo \"$BODY\"");
        sb.AppendLine("    exit 1");
        sb.AppendLine("fi");
        sb.AppendLine();
        sb.AppendLine($"echo \"Connector '{resource.Name}' is ready.\"");

        return sb.ToString();
    }

    private static string BuildMasterScript(IReadOnlyList<PostgresDebeziumConnectorResource> connectors)
    {
        var sb = new StringBuilder();

        sb.AppendLine("#!/bin/bash");
        sb.AppendLine("#");
        sb.AppendLine("# Master script to create all Kafka Connect connectors");
        sb.AppendLine("# Generated by Aspire AppHost during publish");
        sb.AppendLine("#");
        sb.AppendLine("# Usage:");
        sb.AppendLine("#   ./create-all-connectors.sh");
        sb.AppendLine("#   KAFKA_CONNECT_URL=http://localhost:8083 ./create-all-connectors.sh");
        sb.AppendLine("#   SCHEMA_REGISTRY_URL=http://localhost:8081 ./create-all-connectors.sh");
        sb.AppendLine("#");
        sb.AppendLine();
        sb.AppendLine("set -euo pipefail");
        sb.AppendLine();
        sb.AppendLine("SCRIPT_DIR=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")\" && pwd)\"");
        sb.AppendLine();
        sb.AppendLine("echo \"========================================\"");
        sb.AppendLine("echo \"  Kafka Connect Connector Setup\"");
        sb.AppendLine("echo \"========================================\"");
        sb.AppendLine("echo \"\"");
        sb.AppendLine();

        foreach (var resource in connectors)
        {
            sb.AppendLine("echo \"--- Processing connector: " + resource.Name + " ---\"");
            sb.AppendLine($"if [ -f \"$SCRIPT_DIR/{resource.Name}.sh\" ]; then");
            sb.AppendLine($"    bash \"$SCRIPT_DIR/{resource.Name}.sh\"");
            sb.AppendLine($"    echo \"\"");
            sb.AppendLine("else");
            sb.AppendLine($"    echo \"WARNING: Script for '{resource.Name}' not found, skipping.\"");
            sb.AppendLine("fi");
            sb.AppendLine();
        }

        sb.AppendLine("echo \"========================================\"");
        sb.AppendLine("echo \"  All connectors processed\"");
        sb.AppendLine("echo \"========================================\"");

        return sb.ToString();
    }
}
