namespace Kafka.Client.Consumers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    public class ConsumerTopicMetrics
    {

        public IMeter MessageRate { get; private set; }

        public IMeter ByteRate { get; private set; }

        public ConsumerTopicMetrics(ClientIdAndTopic metricId)
        {
            this.MessageRate = MetersFactory.NewMeter(metricId + "MessagesPerSec", "messages", TimeSpan.FromSeconds(1));
            this.ByteRate = MetersFactory.NewMeter(metricId + "BytesPerSec", "bytes", TimeSpan.FromSeconds(1));
        }
    }

    public class ConsumerTopicStats
    {
        public string ClientId { get; private set; }

        private Func<ClientIdAndTopic, ConsumerTopicMetrics> valueFactory;

        private Pool<ClientIdAndTopic, ConsumerTopicMetrics> stats;

        private ConsumerTopicMetrics allTopicStats;

        public ConsumerTopicStats(string clientId)
        {
            this.ClientId = clientId;
            this.valueFactory = k => new ConsumerTopicMetrics(k);
            this.stats = new Pool<ClientIdAndTopic, ConsumerTopicMetrics>(valueFactory);
            this.allTopicStats = new ConsumerTopicMetrics(new ClientIdAndTopic(clientId, "AllTopics"));
        }

        public ConsumerTopicMetrics GetConsumerAllTopicStats()
        {
            return allTopicStats;
        }

        public ConsumerTopicMetrics GetConsumerTopicStats(string topic)
        {
            return stats.GetAndMaybePut(new ClientIdAndTopic(ClientId, topic + "-"));
        }
    }


    /// <summary>
    /// Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
    /// </summary>
    public static class ConsumerTopicStatsRegistry
    {
        private static readonly Func<string, ConsumerTopicStats> valueFactory = k => new ConsumerTopicStats(k);

        private static readonly Pool<string, ConsumerTopicStats> globalStats = new Pool<string, ConsumerTopicStats>(valueFactory);

        public static ConsumerTopicStats GetConsumerTopicStat(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }
    }
}