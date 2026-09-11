using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Represents a single API registration with the Swagger UI builder.
/// </summary>
/// <param name="Endpoint">The endpoint reference for the API resource.</param>
/// <param name="DisplayName">Human-friendly display name shown in the Swagger UI selector.</param>
/// <param name="OpenApiPath">Relative path to the OpenAPI document (e.g. <c>/openapi/v1.json</c>).</param>
public record SwaggerApi(EndpointReference Endpoint, string DisplayName, string OpenApiPath);
