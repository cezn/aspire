using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Extension methods for Aspire resource URL configuration.
/// </summary>
public static class ResourceUrlExtensions
{
    /// <summary>
    /// Subscribes to <see cref="BeforeResourceStartedEvent"/> and configures all resources
    /// so that URLs without a custom <see cref="ResourceUrlAnnotation.DisplayText"/> are
    /// shown in the Details panel only (hidden from the resource list).
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <returns>The same builder for chaining.</returns>
    public static IDistributedApplicationBuilder HideDefaultResourceUrls(this IDistributedApplicationBuilder builder)
    {
        builder.Eventing.Subscribe<BeforeResourceStartedEvent>(
            async (e, ct) =>
            {
                foreach (var url in e.Resource.Annotations.OfType<ResourceUrlAnnotation>())
                {
                    if (url.DisplayText is null)
                    {
                        url.DisplayLocation = UrlDisplayLocation.DetailsOnly;
                    }
                }
            });

        return builder;
    }
}
