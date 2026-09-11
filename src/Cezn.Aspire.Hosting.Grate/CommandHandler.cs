using System.Diagnostics;
using System.Text;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Logging;

namespace Cezn.Aspire.Hosting;

internal static class CommandHandler
{
    private const string GrateToolPackageId = "grate";

    internal static async Task<ExecuteCommandResult> ExecuteGrateCommandAsync(
        GrateMigrationResource resource,
        string mode,
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

            var connectionString = await ResolveConnectionStringAsync(resource, notificationService, cancellationToken)
                .ConfigureAwait(false);

            var adminConnectionString = DeriveAdminConnectionString(connectionString, resource.DatabaseType);

            logger.LogInformation(
                "Running grate {Mode} for resource '{ResourceName}' against {DatabaseType} database...",
                mode,
                resource.Name,
                resource.DatabaseType
            );

            var args = new StringBuilder();
            args.Append("tool run " + GrateToolPackageId + " --");
            args.Append(" --connectionstring \"");
            args.Append(connectionString.Replace("\"", "\\\""));
            args.Append("\"");
            args.Append(" --adminconnectionstring \"");
            args.Append((adminConnectionString ?? "").Replace("\"", "\\\""));
            args.Append("\"");
            args.Append(" --files \"").Append(resource.MigrationsPath).Append("\"");
            args.Append(" --databasetype ").Append(resource.DatabaseType);
            args.Append(" --noninteractive");

            if (resource.CreateDatabase)
                args.Append(" --create");
            if (!string.IsNullOrEmpty(resource.Environment))
                args.Append(" --env ").Append(resource.Environment);
            if (!string.IsNullOrEmpty(resource.OutputPath))
                args.Append(" --output \"").Append(resource.OutputPath).Append("\"");
            if (mode == "status")
                args.Append(" --isuptodate");

            var startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = args.ToString(),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };

            logger.LogInformation("Starting grate with arguments: dotnet {Arguments}", startInfo.Arguments);
            var process = Process.Start(startInfo);
            if (process is null)
            {
                return new ExecuteCommandResult { Success = false, Message = "Failed to start grate tool." };
            }

            var stdoutTask = StreamOutputAsync(process.StandardOutput, logger, false, cancellationToken);
            var stderrTask = StreamOutputAsync(process.StandardError, logger, true, cancellationToken);

            await Task.WhenAll(stdoutTask, stderrTask).ConfigureAwait(false);
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

            var success = process.ExitCode == 0;

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
                    Message = "Grate exited with code " + process.ExitCode + ".",
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

    static async Task<string> ResolveConnectionStringAsync(
        GrateMigrationResource resource,
        ResourceNotificationService notificationService,
        CancellationToken cancellationToken
    )
    {
        var dbResource = resource.DatabaseResource;
        if (dbResource is null)
        {
            throw new InvalidOperationException(
                "No database resource configured for migration resource '" + resource.Name + "'."
            );
        }

        // Wait for the database resource to have a connection string available
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutTokenSource.Token
        );

        // Use a local holder (captured by the callback) instead of a static field
        // to avoid race conditions when multiple migration resources resolve
        // connection strings concurrently.
        string? localConnStr = null;
        var lockObj = new object();

        try
        {
            // Wait for the connection string to be available by checking resource events
            await notificationService
                .WaitForResourceAsync(
                    dbResource.Name,
                    (ResourceEvent notification) =>
                    {
                        // Check if the snapshot has a connectionString property with a non-empty value
                        if (notification.Snapshot?.Properties is null)
                            return false;

                        foreach (var prop in notification.Snapshot.Properties)
                        {
                            if (
                                prop.Name == "resource.connectionString"
                                && prop.Value is string connStr
                                && !string.IsNullOrEmpty(connStr)
                            )
                            {
                                // Store the connection string in the local holder (closure-scoped, not static)
                                lock (lockObj)
                                {
                                    localConnStr = connStr;
                                }
                                return true;
                            }
                        }
                        return false;
                    },
                    linkedCts.Token
                )
                .ConfigureAwait(false);

            lock (lockObj)
            {
                var connStr = localConnStr;
                if (string.IsNullOrEmpty(connStr))
                {
                    throw new InvalidOperationException(
                        "Connection string was not resolved for database resource '" + dbResource.Name + "'."
                    );
                }
                return connStr;
            }
        }
        catch (OperationCanceledException)
        {
            throw new InvalidOperationException(
                "Timed out waiting for the database connection string for migration resource '" + resource.Name + "'."
            );
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

    static string? DeriveAdminConnectionString(string? connectionString, string databaseType)
    {
        if (string.IsNullOrEmpty(connectionString))
            return null;

        return databaseType switch
        {
            "postgresql" => ReplaceDatabase(connectionString, "postgres"),
            "sqlserver" => ReplaceDatabase(connectionString, "master"),
            "mariadb" => ReplaceDatabase(connectionString, ""),
            _ => connectionString,
        };
    }

    static string ReplaceDatabase(string connectionString, string adminDatabase)
    {
        var builder = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = connectionString };
        if (builder.ContainsKey("Database"))
            builder["Database"] = adminDatabase;
        else if (builder.ContainsKey("dbname"))
            builder["dbname"] = adminDatabase;
        return builder.ConnectionString;
    }
}
