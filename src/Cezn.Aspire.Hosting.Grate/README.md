# Cezn.Aspire.Hosting.Grate

[grate](https://grate-devs.github.io/grate/) SQL migration runner hosting support for Aspire.

## Usage

### Basic PostgreSQL Migrations

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var pg = builder.AddPostgres("pg");
var db = pg.AddDatabase("mydb");

db.AddGrateMigrations("db-migrations", "./migrations")
    .WithEnvironment("local")
    .RunMigrationsOnStart();

builder.Build().Run();
```

### With Environment-Specific Scripts

```csharp
db.AddGrateMigrations("db-migrations", "./migrations")
    .WithEnvironment("dev");
```

### Manual Migration Execution

```csharp
// Add migrations without auto-running on start
var migrations = db.AddGrateMigrations("db-migrations", "./migrations");

// Migrations can be triggered manually via the Aspire dashboard command "Run Migrations"
```

### Wait for Migrations

```csharp
var migrations = db.AddGrateMigrations("db-migrations", "./migrations")
    .RunMigrationsOnStart();

var myService = builder.AddProject<MyService>()
    .WaitFor(migrations);
```

## Features

- **Automatic database creation**: grate creates the target database if it doesn't exist (controlled by `CreateDatabase` property).
- **Environment-specific scripts**: Use `.WithEnvironment()` to enable grate's environment-aware script filtering.
- **Startup migrations**: Use `.RunMigrationsOnStart()` to apply migrations automatically when the AppHost starts.
- **Dashboard commands**: "Run Migrations" and "Migration Status" commands are available in the Aspire dashboard.

## Database Types

The PostgreSQL overload (`PostgresDatabaseResource.AddGrateMigrations`) defaults to `postgresql` database type.
For other database types, use the `IDistributedApplicationBuilder.AddGrateMigrations` overload with the `databaseType` parameter.

Supported database types: `postgresql`, `sqlserver`, `sqlite`, `mariadb`, `oracle`.
