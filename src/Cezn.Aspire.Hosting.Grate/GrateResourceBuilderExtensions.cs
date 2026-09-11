using System.Diagnostics;
using System.Text;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Postgres;
using Cezn.Aspire.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using BeforeStartEvent = Aspire.Hosting.ApplicationModel.BeforeStartEvent;

namespace Aspire.Hosting;

/// <summary>
/// Extension methods for adding grate SQL migration resources to Aspire applications.
/// </summary>
public static class GrateResourceBuilderExtensions
{
    /// <summary>
    /// Adds a grate migration resource to a PostgreSQL database.
    /// </summary>
    public static IResourceBuilder<GrateMigrationResource> AddGrateMigrations(
        this IResourceBuilder<PostgresDatabaseResource> builder,
        [ResourceName] string name,
        string migrationsPath
    )
    {
        var resource = new GrateMigrationResource(name, "postgresql")
        {
            MigrationsPath = Path.GetFullPath(migrationsPath, builder.ApplicationBuilder.AppHostDirectory),
            OutputPath = Path.Combine(Path.GetTempPath(), ".grate", name),
            DatabaseResource = builder.Resource,
        };

        var resourceBuilder = builder
            .ApplicationBuilder.AddResource(resource)
            .WithParentRelationship(builder)
            .WithInitialState(new CustomResourceSnapshot { ResourceType = "GrateMigration", Properties = [] })
            .WithIconName("Database")
            .WaitFor(builder);

        resourceBuilder.WithCommand(
            name: "grate-migrate",
            displayName: "Run Migrations",
            executeCommand: context => HandleCommandAsync(context, resource, "migrate"),
            commandOptions: new CommandOptions
            {
                Description = "Apply pending grate migrations to the database",
                IconName = "ArrowSync",
                IconVariant = IconVariant.Regular,
                UpdateState = context => GetCommandState(resource),
            }
        );

        resourceBuilder.WithCommand(
            name: "grate-status",
            displayName: "Migration Status",
            executeCommand: context => HandleCommandAsync(context, resource, "status"),
            commandOptions: new CommandOptions
            {
                Description = "Show the current migration status of the database",
                IconName = "Info",
                IconVariant = IconVariant.Regular,
                UpdateState = context => GetCommandState(resource),
            }
        );

        return resourceBuilder;
    }

    /// <summary>
    /// Configures the grate migration resource to run migrations automatically on AppHost startup.
    /// </summary>
    public static IResourceBuilder<GrateMigrationResource> RunMigrationsOnStart(
        this IResourceBuilder<GrateMigrationResource> builder
    )
    {
        var migrationResource = builder.Resource;
        migrationResource.RunOnStart = true;

        builder.ApplicationBuilder.Eventing.Subscribe<ResourceReadyEvent>(
            builder.Resource.DatabaseResource!,
            (@event, ct) => HandleDatabaseReadyAsync(@event.Services, migrationResource, ct)
        );

        return builder;
    }

    /// <summary>
    /// Sets the grate environment name for environment-specific script filtering.
    /// </summary>
    public static IResourceBuilder<GrateMigrationResource> WithEnvironment(
        this IResourceBuilder<GrateMigrationResource> builder,
        string environment
    )
    {
        builder.Resource.Environment = environment;
        return builder;
    }

    static async Task HandleDatabaseReadyAsync(
        IServiceProvider serviceProvider,
        GrateMigrationResource migrationResource,
        CancellationToken cancellationToken
    )
    {
        var resourceLoggerService = serviceProvider.GetRequiredService<ResourceLoggerService>();
        var logger = resourceLoggerService.GetLogger(migrationResource);

        try
        {
            var notificationService = serviceProvider.GetRequiredService<ResourceNotificationService>();
            var result = await CommandHandler.ExecuteGrateCommandAsync(
                migrationResource,
                "migrate",
                logger,
                notificationService,
                cancellationToken
            );

            if (!result.Success)
            {
                logger.LogError(
                    "Grate migration on startup failed for resource '{ResourceName}'. {ErrorMessage}",
                    migrationResource.Name,
                    result.Message ?? ""
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Grate migration on startup failed for resource '{ResourceName}'.",
                migrationResource.Name
            );
        }
    }

    static async Task<ExecuteCommandResult> HandleCommandAsync(
        ExecuteCommandContext context,
        GrateMigrationResource resource,
        string mode
    ) =>
        await CommandHandler.ExecuteGrateCommandAsync(
            resource,
            mode,
            context.Logger,
            context.ServiceProvider.GetRequiredService<ResourceNotificationService>(),
            context.CancellationToken
        );

    static ResourceCommandState GetCommandState(GrateMigrationResource resource) =>
        resource.IsExecutingCommand ? ResourceCommandState.Disabled : ResourceCommandState.Enabled;
}
