using System.Diagnostics;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.ApplicationModel.Docker;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for adding a Kafka Connect container resource to an Aspire application.
/// </summary>
public static class KafkaConnectBuilderExtensions
{
    /// <summary>
    /// Adds a Kafka Connect container resource to the application.
    /// A <c>KafkaServerResource</c> should be referenced via <see cref="WithKafka"/> or <see cref="WithReference{T}"/>.
    /// </summary>
    /// <param name="builder">The Aspire application builder.</param>
    /// <param name="name">The logical resource name.</param>
    /// <returns>A resource builder for the Kafka Connect instance.</returns>
    public static IResourceBuilder<KafkaConnectResource> AddKafkaConnect(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var resource = new KafkaConnectResource(name);

        // Use a temp directory as the Docker build context.
        // The Dockerfile contains no COPY/ADD of local files (all deps are downloaded from URLs),
        // so the context content is irrelevant. This avoids any dependency on the package's filesystem location.
        var contextPath = Path.Combine(Path.GetTempPath(), ".aspire-kafka-connect-context");
        Directory.CreateDirectory(contextPath);

        return builder
            .AddResource(resource)
            .WithImage(KafkaConnectContainerImageTags.Image, KafkaConnectContainerImageTags.Tag)
            .WithDockerfileFactory(contextPath, ctx => KafkaConnectDockerfile.Content)
            .WithHttpEndpoint(name: KafkaConnectResource.HttpEndpointName, targetPort: 8083)
            .WithHttpHealthCheck("/health");
    }

    /// <summary>
    /// Configures the Kafka Connect container to connect to a Kafka broker.
    /// </summary>
    /// <param name="builder">The Kafka Connect resource builder.</param>
    /// <param name="kafka">The Kafka broker resource to connect to.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<KafkaConnectResource> WithKafka(
        this IResourceBuilder<KafkaConnectResource> builder,
        IResourceBuilder<KafkaServerResource> kafka
    )
    {
        var plaintextEndpoint = kafka.Resource.InternalEndpoint;
        var httpEndpoint = builder.Resource.HttpEndpoint;

        return builder
            .WithEnvironment(ctx =>
            {
                var env = ctx.EnvironmentVariables;

                // Connect classpath
                env["CONNECT_PLUGIN_PATH"] = "/usr/share/java,/usr/share/confluent-hub-components";

                // Converters
                env["CONNECT_KEY_CONVERTER"] = "org.apache.kafka.connect.converters.ByteArrayConverter";
                env["CONNECT_VALUE_CONVERTER"] = "org.apache.kafka.connect.converters.ByteArrayConverter";

                // Bootstrap servers
                env["CONNECT_BOOTSTRAP_SERVERS"] = ReferenceExpression.Create(
                    $"PLAINTEXT://{plaintextEndpoint.Property(EndpointProperty.HostAndPort)}"
                );

                // GC
                env["CONNECT_GC_LOG_ENABLED"] = "false";
                env["CONNECT_HEAP_OPTS"] = "-Xms256M -Xmx256M";

                // REST API
                env["CONNECT_REST_ADVERTISED_HOST_NAME"] = httpEndpoint.Property(EndpointProperty.Host);
                env["CONNECT_REST_PORT"] = httpEndpoint.Property(EndpointProperty.Port);

                // Distributed config topics
                env["CONNECT_GROUP_ID"] = "docker-connect-group";
                env["CONNECT_CONFIG_STORAGE_TOPIC"] = "docker-connect-configs";
                env["CONNECT_OFFSET_STORAGE_TOPIC"] = "docker-connect-offsets";
                env["CONNECT_STATUS_STORAGE_TOPIC"] = "docker-connect-status";
                env["CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR"] = "1";
                env["CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR"] = "1";
                env["CONNECT_STATUS_STORAGE_REPLICATION_FACTOR"] = "1";
                env["CONNECT_CONFIG_STORAGE_PARTITIONS"] = "1";
                env["CONNECT_OFFSET_STORAGE_PARTITIONS"] = "1";
                env["CONNECT_STATUS_STORAGE_PARTITIONS"] = "1";

                // Logging
                env["CONNECT_LOG_LEVEL"] = "info";
            });
    }

    /// <summary>
    /// Configures OpenTelemetry environment variables for a Kafka Connect
    /// container resource and wires it to the Aspire dashboard OTLP exporter.
    /// </summary>
    /// <param name="builder">The Kafka Connect resource builder.</param>
    /// <returns>The resource builder for further chaining.</returns>
    public static IResourceBuilder<KafkaConnectResource> WithOtel(this IResourceBuilder<KafkaConnectResource> builder)
    {
        return builder
            .WithEnvironment(ctx =>
            {
                var env = ctx.EnvironmentVariables;

                env["CONNECT_PRODUCER_INTERCEPTOR_CLASSES"] = "io.debezium.tracing.DebeziumTracingProducerInterceptor";
                env["ENABLE_OTEL"] = "true";
                env["KAFKA_OPTS"] =
                    "-javaagent:/otel/opentelemetry-javaagent.jar -Dotel.instrumentation.kafka.enabled=false";
                env["OTEL_RESOURCE_ATTRIBUTES"] = "service.version=1.0,deployment.environment=production";
                env["OTEL_PROPAGATORS"] = "tracecontext";
                env["OTEL_INSTRUMENTATION_COMMON_DEFAULT_ENABLED"] = "true";
            })
            .WithOtlpExporter()
            .WithDeveloperCertificateTrust(true)
            .WithCertificateTrustConfiguration(async ctx =>
            {
                ctx.EnvironmentVariables["ASPIRE_CERT_PATH"] = ctx.CertificateBundlePath;
                ctx.EnvironmentVariables["KAFKA_OPTS"] =
                    "-javaagent:/otel/opentelemetry-javaagent.jar "
                    + "-Dotel.instrumentation.kafka.enabled=false "
                    + "-Djavax.net.ssl.trustStore=/tmp/aspire-truststore.p12 "
                    + "-Djavax.net.ssl.trustStoreType=PKCS12 "
                    + "-Djavax.net.ssl.trustStorePassword=changeit";
            })
            .WithArgs(
                "bash",
                "-c",
                "export CLASSPATH=$CLASSPATH:/usr/share/java/otel-lib/*; "
                    + "if [ -n \"$ASPIRE_CERT_PATH\" ] && [ -f \"$ASPIRE_CERT_PATH\" ]; then "
                    + "keytool -importcert -noprompt -trustcacerts -file \"$ASPIRE_CERT_PATH\" "
                    + "-keystore /tmp/aspire-truststore.p12 -storetype PKCS12 -storepass changeit -alias aspire-dev-cert; fi; "
                    + "/etc/confluent/docker/run"
            );
    }
}
