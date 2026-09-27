using System.Diagnostics;
using System.Text;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Logging;

namespace Cezn.Aspire.Hosting;

internal static class KafkaTopicCommandHandler
{
    internal static async Task<ExecuteCommandResult> ExecuteCreateTopicAsync(
        KafkaTopicResource resource,
        ILogger logger,
        ResourceNotificationService notificationService,
        CancellationToken cancellationToken
    )
    {
        resource.IsExecutingCommand = true;

        try
        {
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s =>
                        s with
                        {
                            State = KnownResourceStates.Running,
                            StartTimeStamp = DateTime.UtcNow,
                            StopTimeStamp = null,
                        }
                )
                .ConfigureAwait(false);

            var containerId = await ResolveKafkaContainerIdAsync(resource, notificationService, cancellationToken)
                .ConfigureAwait(false);

            logger.LogInformation(
                "Creating Kafka topic '{TopicName}' (partitions: {Partitions}, replication factor: {ReplicationFactor}) on Kafka container '{ContainerId}'...",
                resource.TopicName,
                resource.Partitions,
                resource.ReplicationFactor,
                containerId
            );

            var args = new StringBuilder();
            args.Append("exec -e KAFKA_JMX_OPTS=\"\" ");
            args.Append(containerId);
            args.Append(" kafka-topics --bootstrap-server localhost:9092 --create");
            args.Append(" --topic \"").Append(resource.TopicName.Replace("\"", "\\\"")).Append("\"");
            args.Append(" --partitions ").Append(resource.Partitions);
            args.Append(" --replication-factor ").Append(resource.ReplicationFactor);
            foreach (var (key, value) in resource.Configs)
            {
                args.Append(" --config ");
                args.Append(key.Replace("\"", "\\\""));
                args.Append('=');
                args.Append(value.Replace("\"", "\\\""));
            }
            args.Append(" --if-not-exists");

            // The broker may not accept connections yet when the container is reported
            // as running, so retry the create command until it succeeds or times out.
            var success = false;
            var lastExitCode = -1;
            var deadline = DateTime.UtcNow.AddMinutes(3);
            while (DateTime.UtcNow < deadline)
            {
                var startInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = args.ToString(),
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                };

                logger.LogInformation("Creating topic with arguments: docker {Arguments}", startInfo.Arguments);
                var process = Process.Start(startInfo);
                if (process is null)
                {
                    return new ExecuteCommandResult { Success = false, Message = "Failed to start docker process." };
                }

                var stdoutTask = StreamOutputAsync(process.StandardOutput, logger, false, cancellationToken);
                var stderrTask = StreamOutputAsync(process.StandardError, logger, true, cancellationToken);

                await Task.WhenAll(stdoutTask, stderrTask).ConfigureAwait(false);
                await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

                lastExitCode = process.ExitCode;
                if (process.ExitCode == 0)
                {
                    success = true;
                    break;
                }

                logger.LogWarning(
                    "Kafka topic creation attempt failed (exit code {ExitCode}), retrying in 5s...",
                    process.ExitCode
                );
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            }

            // 'IsExecutingCommand' must be set before updating resource's state. Only then command status is refreshed.
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s =>
                        s with
                        {
                            State = success ? KnownResourceStates.Finished : KnownResourceStates.FailedToStart,
                            StopTimeStamp = DateTime.UtcNow,
                        }
                )
                .ConfigureAwait(false);

            return success
                ? CommandResults.Success()
                : new ExecuteCommandResult
                {
                    Success = false,
                    Message = "docker exited with code " + lastExitCode + ".",
                };
        }
        catch (OperationCanceledException)
        {
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s => s with { State = KnownResourceStates.FailedToStart, StopTimeStamp = DateTime.UtcNow }
                )
                .ConfigureAwait(false);
            return CommandResults.Canceled();
        }
        catch (Exception ex)
        {
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s => s with { State = KnownResourceStates.FailedToStart, StopTimeStamp = DateTime.UtcNow }
                )
                .ConfigureAwait(false);
            return new ExecuteCommandResult { Success = false, Message = ex.Message };
        }
    }

    internal static async Task<ExecuteCommandResult> ExecuteClearTopicAsync(
        KafkaTopicResource resource,
        ILogger logger,
        ResourceNotificationService notificationService,
        CancellationToken cancellationToken
    )
    {
        resource.IsExecutingCommand = true;

        try
        {
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s =>
                        s with
                        {
                            State = KnownResourceStates.Running,
                            StartTimeStamp = DateTime.UtcNow,
                            StopTimeStamp = null,
                        }
                )
                .ConfigureAwait(false);

            var containerId = await ResolveKafkaContainerIdAsync(resource, notificationService, cancellationToken)
                .ConfigureAwait(false);

            logger.LogInformation(
                "Clearing Kafka topic '{TopicName}' (delete + recreate) on Kafka container '{ContainerId}'...",
                resource.TopicName,
                containerId
            );

            var escapedTopic = resource.TopicName.Replace("\"", "\\\"");
            var deleteArgs = new StringBuilder();
            deleteArgs.Append("exec -e KAFKA_JMX_OPTS=\"\" ");
            deleteArgs.Append(containerId);
            deleteArgs.Append(" kafka-topics --bootstrap-server localhost:9092 --delete");
            deleteArgs.Append(" --topic \"").Append(escapedTopic).Append("\"");

            var deleteResult = await RunDockerWithRetryAsync(
                deleteArgs.ToString(),
                "Kafka topic deletion",
                logger,
                cancellationToken
            );
            if (!deleteResult.Success)
            {
                return deleteResult;
            }

            var createArgs = new StringBuilder();
            createArgs.Append("exec -e KAFKA_JMX_OPTS=\"\" ");
            createArgs.Append(containerId);
            createArgs.Append(" kafka-topics --bootstrap-server localhost:9092 --create");
            createArgs.Append(" --topic \"").Append(escapedTopic).Append("\"");
            createArgs.Append(" --partitions ").Append(resource.Partitions);
            createArgs.Append(" --replication-factor ").Append(resource.ReplicationFactor);
            foreach (var (key, value) in resource.Configs)
            {
                createArgs.Append(" --config ");
                createArgs.Append(key.Replace("\"", "\\\""));
                createArgs.Append('=');
                createArgs.Append(value.Replace("\"", "\\\""));
            }

            var createResult = await RunDockerWithRetryAsync(
                createArgs.ToString(),
                "Kafka topic recreation",
                logger,
                cancellationToken
            );

            // 'IsExecutingCommand' must be set before updating resource's state. Only then command status is refreshed.
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s =>
                        s with
                        {
                            State = createResult.Success
                                ? KnownResourceStates.Finished
                                : KnownResourceStates.FailedToStart,
                            StopTimeStamp = DateTime.UtcNow,
                        }
                )
                .ConfigureAwait(false);

            return createResult;
        }
        catch (OperationCanceledException)
        {
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s => s with { State = KnownResourceStates.FailedToStart, StopTimeStamp = DateTime.UtcNow }
                )
                .ConfigureAwait(false);
            return CommandResults.Canceled();
        }
        catch (Exception ex)
        {
            resource.IsExecutingCommand = false;
            await notificationService
                .PublishUpdateAsync(
                    resource,
                    s => s with { State = KnownResourceStates.FailedToStart, StopTimeStamp = DateTime.UtcNow }
                )
                .ConfigureAwait(false);
            return new ExecuteCommandResult { Success = false, Message = ex.Message };
        }
    }

    static async Task<ExecuteCommandResult> RunDockerWithRetryAsync(
        string dockerArguments,
        string operationDescription,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        // The broker may not accept connections yet when the container is reported
        // as running, so retry the command until it succeeds or times out.
        var lastExitCode = -1;
        var deadline = DateTime.UtcNow.AddMinutes(3);
        while (DateTime.UtcNow < deadline)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = dockerArguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };

            logger.LogInformation(
                "{Operation} with arguments: docker {Arguments}",
                operationDescription,
                startInfo.Arguments
            );
            var process = Process.Start(startInfo);
            if (process is null)
            {
                return new ExecuteCommandResult { Success = false, Message = "Failed to start docker process." };
            }

            var stdoutTask = StreamOutputAsync(process.StandardOutput, logger, false, cancellationToken);
            var stderrTask = StreamOutputAsync(process.StandardError, logger, true, cancellationToken);

            await Task.WhenAll(stdoutTask, stderrTask).ConfigureAwait(false);
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

            lastExitCode = process.ExitCode;
            if (process.ExitCode == 0)
            {
                return CommandResults.Success();
            }

            logger.LogWarning(
                "{Operation} attempt failed (exit code {ExitCode}), retrying in 5s...",
                operationDescription,
                process.ExitCode
            );
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        }

        return new ExecuteCommandResult { Success = false, Message = "docker exited with code " + lastExitCode + "." };
    }

    static async Task<string> ResolveKafkaContainerIdAsync(
        KafkaTopicResource resource,
        ResourceNotificationService notificationService,
        CancellationToken cancellationToken
    )
    {
        var kafkaResource = resource.KafkaResource;
        if (kafkaResource is null)
        {
            throw new InvalidOperationException(
                "No Kafka resource configured for topic resource '" + resource.Name + "'."
            );
        }

        // Wait for the Kafka resource to be ready first.
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutTokenSource.Token
        );

        await notificationService
            .WaitForResourceAsync(kafkaResource.Name, KnownResourceStates.Running, linkedCts.Token)
            .ConfigureAwait(false);

        // Find the container by the Aspire naming convention: <resource-name>-<hash>.
        // Other resources (e.g. kafka-connect, kafka-console) share the same prefix,
        // so match only the container whose suffix after '<resource-name>-' contains
        // no further dash (i.e. it is the bare Aspire hash).
        var prefix = kafkaResource.Name + "-";
        for (var attempt = 0; ; attempt++)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "ps --format {{.Names}} --filter name=^" + kafkaResource.Name + "-",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };

            var process = Process.Start(startInfo);
            if (process is null)
            {
                throw new InvalidOperationException("Failed to start docker process.");
            }

            var output = await process.StandardOutput.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

            var containerId = output
                .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .FirstOrDefault(name =>
                    name.StartsWith(prefix, StringComparison.Ordinal) && !name[prefix.Length..].Contains('-')
                );

            if (!string.IsNullOrEmpty(containerId))
            {
                return containerId;
            }

            if (attempt >= 10)
            {
                throw new InvalidOperationException(
                    "No running Kafka container found for resource '" + kafkaResource.Name + "'."
                );
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
        }
    }

    static async Task StreamOutputAsync(
        StreamReader reader,
        ILogger logger,
        bool isError,
        CancellationToken cancellationToken
    )
    {
        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false)) != null)
        {
            if (isError)
                logger.LogWarning(line);
            else
                logger.LogInformation(line);
        }
    }
}
