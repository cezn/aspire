namespace Cezn.Aspire.Hosting;

/// <summary>
/// Provides container image tags for Confluent Kafka Connect.
/// </summary>
public static class KafkaConnectContainerImageTags
{
    /// <summary>
    /// The container image name.
    /// </summary>
    public const string Image = "confluentinc/cp-kafka-connect";

    /// <summary>
    /// The container image tag.
    /// </summary>
    public const string Tag = "7.9.2";
}
