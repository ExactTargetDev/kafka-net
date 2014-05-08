namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    public class ProducerTopicMetrics
    {
        public IMeter MessageRate { get; private set; }

        public IMeter ByteRate { get; private set; }

        public IMeter DroppedMessageRate { get; private set; }

        public ProducerTopicMetrics(ClientIdAndTopic metricId)
        {
            this.MessageRate = MetersFactory.NewMeter(metricId + "MessagesPerSec", "messages", TimeSpan.FromSeconds(1));
            this.ByteRate = MetersFactory.NewMeter(metricId + "BytesPerSec", "bytes", TimeSpan.FromSeconds(1));
            this.DroppedMessageRate = MetersFactory.NewMeter(
                metricId + "DroppedMessagesPerSec", "drops", TimeSpan.FromSeconds(1));
        }
    }

    /// <summary>
    /// Tracks metrics for each topic the given producer client has produced data to.
    /// </summary>
    public class ProducerTopicStats
    {
        private readonly Func<ClientIdAndTopic, ProducerTopicMetrics> valueFactory;

        private readonly Pool<ClientIdAndTopic, ProducerTopicMetrics> stats;

        private readonly ProducerTopicMetrics allTopicsStats;

        public ProducerTopicStats(string clientId)
        {
            this.ClientId = clientId;
            this.valueFactory = k => new ProducerTopicMetrics(k);
            this.stats = new Pool<ClientIdAndTopic, ProducerTopicMetrics>(this.valueFactory);
            this.allTopicsStats = new ProducerTopicMetrics(new ClientIdAndTopic(this.ClientId, "AllTopics"));
        }

        public ProducerTopicMetrics GetProducerAllTopicsStats()
        {
            return this.allTopicsStats;
        }

        public ProducerTopicMetrics GetProducerTopicStats(string topic)
        {
            return this.stats.GetAndMaybePut(new ClientIdAndTopic(this.ClientId, topic + "-"));
        }

        public string ClientId { get; private set; } 
    }

    /// <summary>
    /// Stores the topic stats information of each producer client in a (clientId -> ProducerTopicStats) map.
    /// </summary>
    public static class ProducerTopicStatsRegistry
    {
        private static Func<string, ProducerTopicStats> valueFactory;

        private static Pool<string, ProducerTopicStats> globalStats;

        static ProducerTopicStatsRegistry()
        {
            valueFactory = k => new ProducerTopicStats(k);
            globalStats = new Pool<string, ProducerTopicStats>(valueFactory);
        }

        public static ProducerTopicStats GetProducerTopicStats(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }
    }
}