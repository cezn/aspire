using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Postgres;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Represents a grate migration resource for running SQL script migrations against a database.
/// </summary>
public sealed class GrateMigrationResource([ResourceName] string name, string databaseType)
    : Resource(name),
        IResourceWithWaitSupport
{
    /// <summary>
    /// Gets the database type for grate (e.g., "postgresql", "sqlserver", "sqlite").
    /// </summary>
    public string DatabaseType { get; } = databaseType;

    /// <summary>
    /// Gets or sets the path to the directory containing SQL migration scripts.
    /// </summary>
    public string? MigrationsPath { get; set; }

    /// <summary>
    /// Gets or sets the grate environment name for environment-specific scripts.
    /// </summary>
    public string? Environment { get; set; }

    /// <summary>
    /// Gets or sets the output directory for grate artifacts (backups, logs, etc.).
    /// </summary>
    public string? OutputPath { get; set; }

    /// <summary>
    /// Gets or sets whether to create the target database if it does not exist.
    /// </summary>
    public bool CreateDatabase { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to run migrations automatically on AppHost startup.
    /// </summary>
    public bool RunOnStart { get; set; }

    /// <summary>
    /// Gets or sets whether a migration command is currently executing.
    /// </summary>
    internal bool IsExecutingCommand { get; set; }

    /// <summary>
    /// Gets or sets the database resource to resolve the connection string from.
    /// </summary>
    internal PostgresDatabaseResource? DatabaseResource { get; set; }
}
