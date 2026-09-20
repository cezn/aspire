using System.Reflection;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for adding a monitoring stack (Prometheus, Grafana and a Kafka
/// JMX exporter) to an Aspire application.
///
/// Mirrors the plain Docker Compose setup in <c>containers/prometheus</c>,
/// <c>containers/grafana</c> and <c>containers/kafka/jmx-exporter</c>:
/// <list type="bullet">
/// <item>Kafka JMX exporter (<c>bitnami/jmx-exporter</c>) scrapes the broker's JMX port.</item>
/// <item>Prometheus scrapes the JMX exporter.</item>
/// <item>Grafana uses Prometheus as its default datasource and ships Kafka dashboards.</item>
/// </list>
/// </summary>
public static class MonitoringBuilderExtensions
{
    private const string PrometheusImage = "prom/prometheus";
    private const string PrometheusTag = "v3.13.1";
    private const string GrafanaImage = "grafana/grafana";
    private const string GrafanaTag = "13.1.1";
    private const string JmxExporterImage = "bitnami/jmx-exporter";
    private const string JmxExporterTag = "latest";

    private const int PrometheusPort = 9090;
    private const int GrafanaPort = 3000;
    private const int JmxExporterPort = 8080;
    private const int KafkaJmxPort = 5555;

    /// <summary>
    /// Enables JMX on a Kafka broker so that a <see cref="AddKafkaJmxExporter"/> can scrape it.
    /// </summary>
    public static IResourceBuilder<KafkaServerResource> WithKafkaJmx(
        this IResourceBuilder<KafkaServerResource> builder,
        int port = KafkaJmxPort
    )
    {
        return builder.WithEnvironment(ctx =>
        {
            ctx.EnvironmentVariables["KAFKA_JMX_OPTS"] =
                $"-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port={port} "
                + "-Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false";
        });
    }

    /// <summary>
    /// Adds a JMX exporter container that scrapes a Kafka broker's JMX port and exposes
    /// Prometheus metrics on port 8080.
    /// </summary>
    public static IResourceBuilder<ContainerResource> AddKafkaJmxExporter(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var configPath = Path.Combine(GetConfigDirectory(), "kafka.yml");

        var jmxExporter = builder
            .AddContainer(name, JmxExporterImage, JmxExporterTag)
            // Explicit small heap: the image's default JVM has no -Xmx (defaults to 25% of host RAM).
            .WithEntrypoint("java")
            .WithArgs("-Xms64m", "-Xmx256m", "-XX:MaxMetaspaceSize=128m", "-jar", "jmx_prometheus_standalone.jar")
            .WithHttpEndpoint(name: "metrics", targetPort: JmxExporterPort)
            .WithHttpHealthCheck("/metrics", endpointName: "metrics")
            .WithContainerFiles(
                "/opt/bitnami/jmx-exporter",
                [new ContainerFile { Name = "kafka.yml", SourcePath = configPath }]
            );

        return jmxExporter;
    }

    /// <summary>
    /// Points the JMX exporter at a Kafka broker. Enables JMX on the broker (if not already
    /// enabled) and rewrites the <c>hostPort</c> in the exporter config to the broker's
    /// internal host and JMX port.
    /// </summary>
    public static IResourceBuilder<ContainerResource> WithKafka(
        this IResourceBuilder<ContainerResource> builder,
        IResourceBuilder<KafkaServerResource> kafka,
        int jmxPort = KafkaJmxPort
    )
    {
        kafka.WithKafkaJmx(jmxPort);

        // The exporter requires positional args: <listen port> <config file>.
        // The JMX target itself comes from the config file (hostPort).
        builder.WithArgs(ctx =>
        {
            ctx.Args.Add(JmxExporterPort.ToString());
            ctx.Args.Add("/opt/bitnami/jmx-exporter/kafka.yml");
        });

        return builder.WithContainerFiles(
            "/opt/bitnami/jmx-exporter",
            async (ctx, ct) =>
            {
                var host = await kafka.Resource.InternalEndpoint.Property(EndpointProperty.Host).GetValueAsync(ct);
                var config = File.ReadAllText(Path.Combine(GetConfigDirectory(), "kafka.yml"))
                    .Replace("hostPort: broker1:5555", $"hostPort: {host}:{jmxPort}");

                return [new ContainerFile { Name = "kafka.yml", Contents = config }];
            }
        );
    }

    /// <summary>
    /// Adds a Prometheus container that scrapes the given JMX exporter.
    /// </summary>
    public static IResourceBuilder<ContainerResource> AddPrometheus(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var prometheus = builder
            .AddContainer(name, PrometheusImage, PrometheusTag)
            .WithHttpEndpoint(name: "http", targetPort: PrometheusPort)
            .WithHttpHealthCheck("/-/ready")
            .WithVolume(name: $"{name}-data", target: "/prometheus")
            .WithArgs(
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                "--web.enable-lifecycle",
                // Required so the OTel collector can push metrics via remote write.
                "--web.enable-remote-write-receiver"
            );

        prometheus.WithUrl(
            $"{new EndpointReference(prometheus.Resource, "http").Property(EndpointProperty.Url)}",
            "Prometheus"
        );

        return prometheus;
    }

    /// <summary>
    /// Configures Prometheus to scrape the given JMX exporter's metrics endpoint.
    /// </summary>
    public static IResourceBuilder<ContainerResource> WithScrapeTarget(
        this IResourceBuilder<ContainerResource> builder,
        IResourceBuilder<ContainerResource> jmxExporter
    )
    {
        return builder.WithContainerFiles(
            "/etc/prometheus",
            async (ctx, ct) =>
            {
                // Resolve the endpoint for the Aspire container network so the target
                // is reachable from inside the Prometheus container (not host-local).
                var target = await jmxExporter
                    .GetEndpoint("metrics", KnownNetworkIdentifiers.DefaultAspireContainerNetwork)
                    .Property(EndpointProperty.HostAndPort)
                    .GetValueAsync(ct);

                var config = $"""
                    global:
                      scrape_interval: 5s
                      scrape_timeout: 3s
                      evaluation_interval: 1m

                    scrape_configs:
                      - job_name: 'kafka-broker'
                        scrape_interval: 15s
                        static_configs:
                          - targets: ['{target}']
                            labels:
                              instance: 'kafka'
                              env: 'development'
                    """;

                return [new ContainerFile { Name = "prometheus.yml", Contents = config }];
            }
        );
    }

    /// <summary>
    /// Adds a Grafana container with Prometheus provisioned as its default datasource and
    /// Kafka dashboards provisioned from the package's <c>config</c> directory.
    /// </summary>
    public static IResourceBuilder<ContainerResource> AddGrafana(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name
    )
    {
        var configDir = GetConfigDirectory();
        var dashboardsDir = Path.Combine(configDir, "dashboards");

        var grafana = builder
            .AddContainer(name, GrafanaImage, GrafanaTag)
            .WithHttpEndpoint(name: "http", targetPort: GrafanaPort)
            .WithHttpHealthCheck("/api/health")
            .WithVolume(name: $"{name}-data", target: "/var/lib/grafana")
            .WithEnvironment("GF_SECURITY_ADMIN_USER", "admin")
            .WithEnvironment("GF_SECURITY_ADMIN_PASSWORD", "admin")
            .WithEnvironment("GF_SECURITY_ADMIN_PASSWORD__CHANGEREQUIRED", "false");

        // Placeholder datasource; call WithPrometheus to point it at a Prometheus resource.
        grafana.WithContainerFiles(
            "/etc/grafana/provisioning/datasources",
            [
                new ContainerFile
                {
                    Name = "prometheus.yml",
                    Contents = """
                    apiVersion: 1

                    datasources:
                      - name: Prometheus
                        uid: prometheus
                        type: prometheus
                        access: proxy
                        url: http://localhost:9090
                        isDefault: true
                        editable: true
                        jsonData:
                          timeInterval: 5s
                    """,
                },
            ]
        );

        // Dashboards: static files shipped with the package.
        if (Directory.Exists(dashboardsDir))
        {
            grafana.WithContainerFiles("/etc/grafana/provisioning/dashboards", dashboardsDir);
        }

        return grafana;
    }

    /// <summary>
    /// Points Grafana's provisioned Prometheus datasource at the given Prometheus resource.
    /// </summary>
    public static IResourceBuilder<ContainerResource> WithPrometheus(
        this IResourceBuilder<ContainerResource> builder,
        IResourceBuilder<ContainerResource> prometheus
    )
    {
        return builder.WithContainerFiles(
            "/etc/grafana/provisioning/datasources",
            async (ctx, ct) =>
            {
                // Resolve the endpoint for the Aspire container network so Grafana
                // can reach Prometheus from inside its container (not host-local).
                var hostAndPort = await prometheus
                    .GetEndpoint("http", KnownNetworkIdentifiers.DefaultAspireContainerNetwork)
                    .Property(EndpointProperty.HostAndPort)
                    .GetValueAsync(ct);

                return
                [
                    new ContainerFile
                    {
                        Name = "prometheus.yml",
                        Contents = $"""
                            apiVersion: 1

                            datasources:
                              - name: Prometheus
                                uid: prometheus
                                type: prometheus
                                access: proxy
                                url: http://{hostAndPort}
                                isDefault: true
                                editable: true
                                jsonData:
                                  timeInterval: 5s
                            """,
                    },
                ];
            }
        );
    }

    /// <summary>
    /// Resolves the <c>config</c> directory shipped with this package (next to the assembly
    /// in the build output, or inside the NuGet package).
    /// </summary>
    private static string GetConfigDirectory()
    {
        var assemblyDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!;
        var configDir = Path.Combine(assemblyDir, "config");
        if (Directory.Exists(configDir))
        {
            return configDir;
        }

        throw new DirectoryNotFoundException(
            $"Monitoring config directory not found at '{configDir}'. "
                + "Ensure the package's 'config' folder was copied to the output."
        );
    }
}
