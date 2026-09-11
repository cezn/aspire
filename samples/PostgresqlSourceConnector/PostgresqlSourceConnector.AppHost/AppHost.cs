using Aspire.Hosting.Docker.Resources.ServiceNodes;
using Cezn.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);
builder.AddDockerComposeEnvironment("compose-dev");

var postgres = builder
    .AddPostgres("postgres")
    .WithImage("postgres", "16-alpine")
    .WithArgs("-c", "wal_level=logical")
    .WithPgAdmin();
var db = postgres.AddDatabase("todos");

var kafka = builder.AddKafka("kafka").WithLifetime(ContainerLifetime.Persistent);

var schemaRegistry = builder.AddSchemaRegistry("schema-registry").WithKafka(kafka);

var kafkaConnect = builder.AddKafkaConnect("kafka-connect").WithKafka(kafka);
kafkaConnect.AddPostgresDebeziumConnector(
    "postgres-cdc",
    db,
    options =>
    {
        options.TopicPrefix = "postgres";
        options.SlotName = "todos_cdc";
        options.PublicationName = "todos_publication";
        options.SnapshotMode = "initial";
        options.TableIncludeList = "public.Todos";
        options.HeartbeatIntervalMs = 5000;
        options.ProvideTransactionMetadata = true;
    }
);

var redpandaConsole = builder
    .AddRedpandaConsole("redpanda-console")
    .WithKafka(kafka)
    .WithKafkaConnect(kafkaConnect)
    .WithSchemaRegistry(schemaRegistry);

var todosApi = builder
    .AddProject<Projects.PostgresqlSourceConnector_TodosApi>("todosapi")
    .WithReference(db)
    .WaitFor(db)
    .WithUrlForEndpoint(
        "https",
        url =>
        {
            url.DisplayText = "Swagger UI";
            url.Url = "/swagger";
        }
    );

var todosApiMigrations = todosApi
    .AddEFMigrations("todosapi-migrations", "PostgresqlSourceConnector.TodosApi.Data.TodoDbContext")
    .RunDatabaseUpdateOnStart()
    .WaitFor(db);

todosApi.WaitFor(todosApiMigrations);

builder.Build().Run();
