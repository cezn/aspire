using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Cezn.Aspire.Hosting;

/// <summary>
/// Represents a Kafka topic that can be created on a Kafka broker.
/// </summary>
public sealed class KafkaTopicResource([ResourceName] string name)
    : Resource(name),
        IResourceWithWaitSupport
{
    /// <summary>
    /// Gets or sets the Kafka topic name. Defaults to the resource name.
    /// </summary>
    public string TopicName { get; set; } = name;

    /// <summary>
    /// Gets or sets the number of partitions for the topic.
    /// </summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// Gets or sets the replication factor for the topic.
    /// </summary>
    public int ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Gets or sets additional topic configurations (e.g. "cleanup.policy" = "compact").
    /// </summary>
    public Dictionary<string, string> Configs { get; } = new();

    /// <summary>
    /// Gets or sets whether the topic is created automatically on AppHost startup.
    /// </summary>
    public bool RunOnStart { get; set; }

    /// <summary>
    /// Gets or sets whether a topic creation command is currently executing.
    /// </summary>
    internal bool IsExecutingCommand { get; set; }

    /// <summary>
    /// Gets or sets the Kafka broker resource to create the topic on.
    /// </summary>
    internal KafkaServerResource? KafkaResource { get; set; }
}
