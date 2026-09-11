using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Container resource for Swagger UI with explicit API registration.
/// </summary>
public class SwaggerUIResource : ContainerResource
{
    internal const string HttpEndpointName = "http";

    public EndpointReference HttpEndpoint => field ??= new(this, HttpEndpointName);

    internal List<SwaggerApi> SwaggerApis = [];

    public SwaggerUIResource(string name)
        : base(name) { }
}

/// <summary>
/// Extension methods for adding Swagger UI and explicitly registering API resources.
/// </summary>
public static class SwaggerUIBuilderExtensions
{
    private const string Image = "swaggerapi/swagger-ui";
    private const string Tag = "v5.31.0";
    internal const int DefaultTargetPort = 8080;

    /// <summary>
    /// Registers this API resource with a Swagger UI builder for explicit discovery,
    /// and configures an environment variable on the API resource
    /// for CORS origin setup.
    /// </summary>
    /// <typeparam name="T">Resource type (must implement <see cref="IResourceWithEndpoints"/> and <see cref="IResourceWithEnvironment"/>).</typeparam>
    /// <param name="builder">The API resource builder.</param>
    /// <param name="swaggerBuilder">The Swagger UI builder returned by <see cref="AddSwaggerUI"/>.</param>
    /// <param name="displayName">Human-friendly name shown in the Swagger UI selector.</param>
    /// <param name="openApiPath">Relative path to the OpenAPI document (default: <c>/openapi/v1.json</c>).</param>
    /// <param name="endpointName">Endpoint name to use (default: <c>"http"</c>).</param>
    /// <param name="environmentVariableName">Environment variable name for the Swagger UI URL (default: <c>"SWAGGER_UI_URL"</c>).</param>
    public static IResourceBuilder<T> WithSwagger<T>(
        this IResourceBuilder<T> builder,
        IResourceBuilder<SwaggerUIResource> swaggerBuilder,
        string? displayName = null,
        string openApiPath = "/openapi/v1.json",
        string? endpointName = null,
        string? environmentVariableName = "SWAGGERUI__URL"
    )
        where T : IResourceWithEndpoints, IResourceWithEnvironment
    {
        displayName ??= builder.Resource.Name;
        endpointName ??= "http";

        swaggerBuilder.WithApi(
            new SwaggerApi(
                builder.GetEndpoint(endpointName, KnownNetworkIdentifiers.LocalhostNetwork),
                displayName,
                openApiPath
            )
        );

        if (!string.IsNullOrWhiteSpace(environmentVariableName))
        {
            builder.WithEnvironment(ctx =>
                ctx.EnvironmentVariables[environmentVariableName] = swaggerBuilder
                    .Resource
                    .HttpEndpoint
            );
        }

        if (displayName?.Trim() != "")
        {
            builder.WithUrl(
                $"{swaggerBuilder.Resource.HttpEndpoint.Property(EndpointProperty.Url)}?urls.primaryName={displayName}",
                $"Swagger UI - {displayName}"
            );
        }

        return builder;
    }

    /// <summary>
    /// Adds a Swagger UI container that uses explicit API registration via
    /// <see cref="WithSwagger{T}"/>. Each API resource calls <c>WithSwagger</c> to register itself.
    ///
    /// The swagger-ui image's built-in configurator (<c>/docker-entrypoint.d/40-swagger-ui.sh</c>)
    /// processes the <c>URLS</c> env var and injects the configuration into <c>swagger-initializer.js</c>
    /// at container startup.
    /// </summary>
    /// <param name="builder">The Aspire application builder.</param>
    /// <param name="name">The logical resource name (default: <c>"swagger-ui"</c>).</param>
    public static IResourceBuilder<SwaggerUIResource> AddSwaggerUI(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name = "swagger-ui"
    ) =>
        builder
            .AddResource(new SwaggerUIResource(name))
            .WithImage(Image, Tag)
            .WithHttpEndpoint(
                name: SwaggerUIResource.HttpEndpointName,
                targetPort: DefaultTargetPort
            )
            .WithEnvironment("URL", "");

    /// <summary>
    /// Registers an API resource with this Swagger UI instance for explicit discovery.
    /// </summary>
    /// <param name="builder">The Swagger UI resource builder.</param>
    /// <param name="api">The Swagger API registration containing endpoint, display name, and OpenAPI path.</param>
    public static IResourceBuilder<SwaggerUIResource> WithApi(
        this IResourceBuilder<SwaggerUIResource> builder,
        SwaggerApi api
    ) =>
        builder.WithEnvironment(ctx =>
        {
            builder.Resource.SwaggerApis.Add(api);
            ctx.EnvironmentVariables["URLS"] = GenerateUrlsEnvVar(builder.Resource.SwaggerApis);
        });

    static ReferenceExpression GenerateUrlsEnvVar(List<SwaggerApi> registrations)
    {
        var expressionBuilder = new ReferenceExpressionBuilder();
        expressionBuilder.AppendLiteral("[");

        foreach (var reg in registrations)
            expressionBuilder.Append(
                $$$"""
                {{ url: "{{{reg.Endpoint}}}{{{reg.OpenApiPath}}}", name: "{{{reg.DisplayName}}}" }},
                """
            );

        expressionBuilder.AppendLiteral("]");

        return expressionBuilder.Build();
    }
}
